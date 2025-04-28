package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

const (
	defaultAudioFormat    = "mp3"
	defaultAudioQuality   = "0"
	defaultDownloadFormat = "bv*+ba/b"
	archiveBaseFilename   = "ytmp3_processed_archive.txt"
	logTimestampFormat    = "2006-01-02_15-04-05"
)

type Config struct {
	DownloadFormat   string
	ConversionFormat string
	LogFilename      string
	KeepFiles        bool
	SkipArchive      bool
	AudioQuality     string
	OutputBaseDir    string
	ArchiveFilepath  string
	Args             []string
	// Keep a reference to the log file handle if open
	LogFileHandle *os.File `json:"-"` // Exclude from json marshal if ever needed
}

type SafeSet struct {
	mu sync.Mutex
	m  map[string]struct{}
}

func (s *SafeSet) Add(item string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.m == nil {
		s.m = make(map[string]struct{})
	}
	if _, exists := s.m[item]; exists {
		return false
	}
	s.m[item] = struct{}{}
	return true
}

func (s *SafeSet) Exists(item string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.m == nil {
		return false
	}
	_, exists := s.m[item]
	return exists
}

func (s *SafeSet) Remove(item string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.m != nil {
		delete(s.m, item)
	}
}

func (s *SafeSet) Load(r io.Reader) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.m == nil {
		s.m = make(map[string]struct{})
	}
	scanner := bufio.NewScanner(r)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line != "" {
			s.m[line] = struct{}{}
		}
	}
	return scanner.Err()
}

type processingResult struct {
	Identifier string
	ItemNumber int
	Error      error
	ArchiveErr error
	StartTime  time.Time
	Duration   time.Duration
}

var (
	processedCount   atomic.Uint64
	skippedCount     atomic.Uint64
	errorCount       atomic.Uint64
	totalItems       int
	startTime        time.Time
	processedArchive SafeSet
	pending          SafeSet
	shutdownSignal   = make(chan os.Signal, 1)
	// Removed logFileHandle global - will pass via Config
	outputMutex sync.Mutex // Retained for atomic start/end blocks if needed, but primarily using log package now
)

func main() {
	// Configure logger for initial setup messages (stderr only, no prefixes yet)
	log.SetOutput(os.Stderr)
	log.SetFlags(0) // Disable default flags for now
	startTime = time.Now()

	cfg, err := parseFlags()
	if err != nil {
		os.Exit(1)
	}

	// Setup logging based on config. Returns the cleanup function.
	logCleanup, err := setupLogging(cfg) // Pass the whole config
	if err != nil {
		log.Fatalf("FATAL: Failed to setup logging: %v", err)
	}
	defer logCleanup() // Ensure log file is closed on exit

	// Check dependencies
	if err := checkDependencies("yt-dlp", "ffmpeg"); err != nil {
		log.Fatalf("FATAL: Dependency check failed: %v", err) // Use log.Fatalf
	}

	// Setup signal handling
	signal.Notify(shutdownSignal, syscall.SIGINT, syscall.SIGTERM)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		sig := <-shutdownSignal
		// Use log.Printf for consistent logging
		log.Printf("\nWARN: Received signal: %v. Shutting down gracefully...", sig)
		cancel()
	}()

	// Initialize archive
	if !cfg.SkipArchive {
		if err := initArchive(cfg.ArchiveFilepath); err != nil {
			log.Fatalf("FATAL: Archive initialization failed (%s): %v", cfg.ArchiveFilepath, err)
		}
		// Use log.Printf
		log.Printf("INFO: Using archive file: %s (%d entries loaded)", cfg.ArchiveFilepath, len(processedArchive.m))
	} else {
		log.Printf("INFO: Skipping archive file check.")
	}

	// Prepare items
	itemsToProcess := deduplicateArgs(cfg.Args)
	totalItems = len(itemsToProcess)
	if totalItems == 0 {
		log.Println("INFO: No URLs or IDs provided.") // Use log.Println
		flag.Usage()
		os.Exit(1)
	}

	log.Printf("INFO: Starting processing for %d unique items at %s", totalItems, startTime.Format("15:04:05"))

	var wg sync.WaitGroup
	results := make(chan processingResult, totalItems)

	// Launch goroutines
	for i, identifier := range itemsToProcess {
		wg.Add(1)
		// Pass the config (which includes the log file handle) to the goroutine
		go processItem(ctx, &wg, cfg, identifier, i+1, results)
	}

	// Waiter goroutine
	go func() {
		wg.Wait()
		close(results)
	}()

	// Process results
	interrupted := false
	for result := range results {
		// Basic handling - detailed logs are now in processItem
		if result.Error != nil && !strings.Contains(result.Error.Error(), "skipped") && !strings.Contains(result.Error.Error(), "cancelled") {
			// Already logged in processItem, just update counter if not skipped/cancelled
		} else if result.Error == nil {
			// Already logged in processItem
		}
		// Counters are updated within processItem now
	}

	// Check for interruption reason
	select {
	case <-ctx.Done():
		// Use logger
		if ctx.Err() != context.Canceled && ctx.Err() != context.DeadlineExceeded { // Check standard context errors
			log.Printf("WARN: Context error during shutdown: %v", ctx.Err())
		}
		// Check if it was our signal
		select {
		case <-shutdownSignal:
			interrupted = true // Flag that shutdown was due to signal
		default:
			// Context cancelled for other reasons, or signal already processed
		}
	default:
		// Processing finished normally
	}

	printSummary(interrupted)
}

func parseFlags() (*Config, error) {
	cfg := &Config{}
	exePath, err := os.Executable()
	if err != nil {
		log.Printf("WARN: Could not determine executable path: %v. Using current directory.", err)
		exePath, err = os.Getwd()
		if err != nil {
			return nil, fmt.Errorf("failed to get current working directory: %w", err)
		}
	}
	baseDir := filepath.Dir(exePath)
	cfg.ArchiveFilepath = filepath.Join(baseDir, archiveBaseFilename)
	cfg.OutputBaseDir = baseDir

	// Define flags using cfg fields directly
	flag.StringVar(&cfg.DownloadFormat, "d", defaultDownloadFormat, "Download format specifier for yt-dlp.")
	flag.StringVar(&cfg.DownloadFormat, "download-format", defaultDownloadFormat, "Download format specifier for yt-dlp.")
	flag.StringVar(&cfg.ConversionFormat, "c", defaultAudioFormat, "Target audio format for conversion.")
	flag.StringVar(&cfg.ConversionFormat, "conversion", defaultAudioFormat, "Target audio format for conversion.")
	flag.StringVar(&cfg.AudioQuality, "audio-quality", defaultAudioQuality, "Audio quality for yt-dlp (--audio-quality).")
	flag.StringVar(&cfg.LogFilename, "l", "", "Log file path. If set, logs app messages AND yt-dlp output to the file (timestamped). Alias: --logs")
	flag.StringVar(&cfg.LogFilename, "logs", "", "Log file path.")
	flag.BoolVar(&cfg.KeepFiles, "k", false, "Keep original downloaded file after processing. Default is delete. Alias: --keep")
	flag.BoolVar(&cfg.KeepFiles, "keep", false, "Keep original downloaded file.")
	flag.BoolVar(&cfg.SkipArchive, "s", false, "Skip checking the archive file. Alias: --skip-archive")
	flag.BoolVar(&cfg.SkipArchive, "skip-archive", false, "Skip checking the archive file.")

	flag.Usage = func() {
		// Use fmt.Fprintf to write to stderr for usage message
		fmt.Fprintf(os.Stderr, `
Usage: %s [OPTIONS] URL/ID...

Downloads YouTube videos/playlists, extracts audio, splits chapters, and saves them.

Options:
`, filepath.Base(os.Args[0]))
		flag.PrintDefaults()
		fmt.Fprintf(os.Stderr, `
Arguments:
  URL/ID...         One or more YouTube URLs, video IDs, playlist URLs, or yt-dlp supported URLs.

Example:
  %s -c flac -k "URL1" "PlaylistURL"

Archive File:
  Processed items recorded in '%s' (unless -s used).
Log File:
  If -l/--logs is used, both app logs and yt-dlp output are saved to the specified file.
`, filepath.Base(os.Args[0]), cfg.ArchiveFilepath)
	}

	flag.Parse()
	cfg.Args = flag.Args()

	// Simple validation (optional)
	// ... (validation logic if needed)

	return cfg, nil
}

// setupLogging configures the main application logger.
// It modifies the Config to store the file handle if opened.
func setupLogging(cfg *Config) (cleanup func(), err error) {
	cleanup = func() {} // Default no-op cleanup

	// Base setup: Log application messages to stderr with timestamps
	log.SetOutput(os.Stderr)
	log.SetFlags(log.Ltime) // Add time prefix to stderr messages

	if cfg.LogFilename == "" {
		// No file logging requested, only stderr logging is active for app messages.
		// yt-dlp will also just use os.Stdout/os.Stderr later.
		return cleanup, nil
	}

	// File logging is requested
	timestamp := time.Now().Format(logTimestampFormat)
	// Ensure directory exists if path includes directories
	logDir := filepath.Dir(cfg.LogFilename)
	if logDir != "." && logDir != "/" {
		if err := os.MkdirAll(logDir, 0755); err != nil {
			return cleanup, fmt.Errorf("failed to create log directory %s: %w", logDir, err)
		}
	}
	// Add timestamp to base filename
	baseName := filepath.Base(cfg.LogFilename)
	ext := filepath.Ext(baseName)
	datedLogFilename := filepath.Join(logDir, fmt.Sprintf("%s_%s%s", strings.TrimSuffix(baseName, ext), timestamp, ext))

	// Attempt to open the log file
	fileHandle, err := os.OpenFile(datedLogFilename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		// Fallback: Log error to stderr but continue without file logging
		log.Printf("ERROR: Failed to open log file %s: %v. Logging to stderr only.", datedLogFilename, err)
		cfg.LogFilename = "" // Indicate file logging failed
		cfg.LogFileHandle = nil
		return cleanup, nil // Return nil error, allowing execution to continue
	}

	// Store the handle in the config
	cfg.LogFileHandle = fileHandle

	// Configure the standard logger to write to both stderr and the file
	multiWriter := io.MultiWriter(os.Stderr, cfg.LogFileHandle)
	log.SetOutput(multiWriter)
	log.SetFlags(log.Ldate | log.Ltime) // Add date and time prefixes

	// Prepare the cleanup function
	cleanup = func() {
		if cfg.LogFileHandle != nil {
			log.Println("INFO: Closing log file.") // This will go to both outputs
			cfg.LogFileHandle.Close()
		}
	}

	log.Printf("INFO: Logging application messages to stderr and file: %s", datedLogFilename)
	return cleanup, nil
}

func checkDependencies(cmds ...string) error {
	missing := []string{}
	for _, cmd := range cmds {
		if _, err := exec.LookPath(cmd); err != nil {
			missing = append(missing, cmd)
		}
	}
	if len(missing) > 0 {
		// Return error instead of fatal logging here
		return fmt.Errorf("required command(s) not found in PATH: %s", strings.Join(missing, ", "))
	}
	log.Println("INFO: Dependencies checked:", strings.Join(cmds, ", "))
	return nil
}

func initArchive(path string) error {
	file, err := os.OpenFile(path, os.O_RDONLY|os.O_CREATE, 0644)
	if err != nil {
		return fmt.Errorf("failed to open/create archive file %s: %w", path, err)
	}
	defer file.Close()

	if err := processedArchive.Load(file); err != nil {
		return fmt.Errorf("error reading archive file %s: %w", path, err)
	}
	return nil
}

func buildYtdlpArgs(cfg *Config, identifier string) []string {
	// Returns only the arguments, not the full command object yet
	args := []string{
		"--ignore-errors",
		"--no-call-home",
		"--color", "always", // Keep color codes even if redirected
		"--progress",
		"--newline",
		// Simplified template for better parsing if needed, but retains info
		"--progress-template", "[download] %(progress._percent_str)s (%(progress._total_bytes_str)s @ %(progress._speed_str)s ETA %(progress._eta_str)s)",
		"--console-title",
		"-f", cfg.DownloadFormat,
		"--extract-audio",
		"--audio-format", cfg.ConversionFormat,
		"--audio-quality", cfg.AudioQuality,
		"--split-chapters",
		"--embed-thumbnail", // Requires ffmpeg and ffprobe for detection, AtomicParsley/mutagen for embedding
		"-o", fmt.Sprintf("chapter:%s/%%(title)s/%%(section_title)s - %%(title)s.%%(ext)s", cfg.OutputBaseDir),
	}
	if cfg.KeepFiles {
		args = append(args, "--keep-video")
		log.Printf("DEBUG: [%s] Will keep original downloaded file.", identifier)
	}
	args = append(args, identifier) // Add identifier last
	return args
}

func processItem(ctx context.Context, wg *sync.WaitGroup, cfg *Config, identifier string, itemNumber int, results chan<- processingResult) {
	defer wg.Done()

	result := processingResult{
		Identifier: identifier,
		ItemNumber: itemNumber,
		StartTime:  time.Now(),
	}
	defer func() {
		result.Duration = time.Since(result.StartTime)
		results <- result // Send result regardless of outcome
	}()

	// Use standard logger for application status messages
	// Use fmt.Sprintf to build the multi-line message, then log it once
	startMsg := fmt.Sprintf("\n╔════ ITEM %d/%d [%s] ════════════════════════\n║ URL: %s\n║ Start: %s\n╚═══════════════════════════════════════════════",
		itemNumber, totalItems, identifier, identifier, result.StartTime.Format("15:04:05"))
	log.Print(startMsg) // Use log.Print to avoid extra prefixes on multi-line string

	// --- Concurrency and Archive Check ---
	if !pending.Add(identifier) {
		log.Printf("WARN: [%s] Item %d/%d skipped (duplicate already in progress).", identifier, itemNumber, totalItems)
		result.Error = fmt.Errorf("skipped (duplicate in progress)")
		errorCount.Add(1)
		return
	}
	defer pending.Remove(identifier)

	if !cfg.SkipArchive && processedArchive.Exists(identifier) {
		log.Printf("INFO: [%s] Item %d/%d skipped (already in archive).", identifier, itemNumber, totalItems)
		skippedCount.Add(1)
		// No error for skipped items
		return
	}

	// --- Prepare yt-dlp Command ---
	args := buildYtdlpArgs(cfg, identifier)
	cmd := exec.CommandContext(ctx, "yt-dlp", args...)

	// --- Setup Output Redirection for yt-dlp ---
	var ytDlpStdoutWriter io.Writer = os.Stdout
	var ytDlpStderrWriter io.Writer = os.Stderr

	if cfg.LogFileHandle != nil {
		// Log file exists, write yt-dlp output to BOTH terminal and file
		ytDlpStdoutWriter = io.MultiWriter(os.Stdout, cfg.LogFileHandle)
		ytDlpStderrWriter = io.MultiWriter(os.Stderr, cfg.LogFileHandle)
		log.Printf("DEBUG: [%s] Redirecting yt-dlp stdout/stderr to terminal and log file.", identifier)
	} else {
		log.Printf("DEBUG: [%s] Redirecting yt-dlp stdout/stderr directly to terminal.", identifier)
	}

	cmd.Stdout = ytDlpStdoutWriter
	cmd.Stderr = ytDlpStderrWriter

	log.Printf("DEBUG: [%s] Running yt-dlp command: yt-dlp %s", identifier, strings.Join(args, " "))

	// --- Run Command ---
	err := cmd.Run()

	// --- Handle Outcome ---
	logMsgSuffix := "" // For the final status log message

	if ctx.Err() == context.Canceled {
		result.Error = fmt.Errorf("cancelled")
		log.Printf("WARN: [%s] Item %d/%d processing cancelled.", identifier, itemNumber, totalItems)
		errorCount.Add(1)
		logMsgSuffix = "(Cancelled)"
	} else if err != nil {
		// Capture specific exit error if possible
		errMsg := fmt.Sprintf("yt-dlp execution failed: %v", err)
		if exitErr, ok := err.(*exec.ExitError); ok {
			errMsg = fmt.Sprintf("yt-dlp execution failed with exit code %d: %v", exitErr.ExitCode(), err)
		}
		result.Error = fmt.Errorf(errMsg)
		log.Printf("ERROR: [%s] Item %d/%d failed: %s", identifier, itemNumber, totalItems, errMsg)
		errorCount.Add(1)
		logMsgSuffix = "(Failed)"
	} else {
		// Success
		log.Printf("INFO: [%s] Item %d/%d processing completed successfully.", identifier, itemNumber, totalItems)
		processedCount.Add(1)
		logMsgSuffix = "(Success)"

		// Add to archive only on success and if not skipped
		if !cfg.SkipArchive {
			if archiveErr := appendToArchive(cfg.ArchiveFilepath, identifier); archiveErr != nil {
				log.Printf("WARN: [%s] Item %d/%d processed, but failed to update archive %s: %v", identifier, itemNumber, totalItems, cfg.ArchiveFilepath, archiveErr)
				result.ArchiveErr = archiveErr // Record archive error separately
				logMsgSuffix = "(Success, Archive Failed)"
			} else {
				log.Printf("DEBUG: [%s] Added to archive.", identifier)
			}
		}
	}

	// Log end message using the application's logger
	endMsg := fmt.Sprintf("╚════ ITEM %d/%d [%s] %s ══════════════════",
		itemNumber, totalItems, identifier, logMsgSuffix)
	log.Println(endMsg)
}

func appendToArchive(path string, identifier string) error {
	if !processedArchive.Add(identifier) {
		log.Printf("DEBUG: [%s] Identifier already present in in-memory archive set (safety check).", identifier)
		return nil
	}

	// Use the LogFileHandle from Config if available for file writing
	// However, appending to archive should likely always use the dedicated archive file,
	// regardless of general logging settings. Let's stick to opening the archive path directly.
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0644)
	if err != nil {
		processedArchive.Remove(identifier) // Rollback in-memory add
		return fmt.Errorf("archive file open failed (%s): %w", path, err)
	}
	defer file.Close()

	if _, err := fmt.Fprintln(file, identifier); err != nil {
		processedArchive.Remove(identifier) // Rollback in-memory add
		return fmt.Errorf("archive file write failed (%s): %w", path, err)
	}

	return nil
}

func deduplicateArgs(args []string) []string {
	if len(args) < 2 {
		return args
	}
	seen := make(map[string]struct{}, len(args))
	unique := make([]string, 0, len(args))
	for _, arg := range args {
		trimmed := strings.TrimSpace(arg)
		if trimmed == "" {
			continue
		}
		if _, exists := seen[trimmed]; !exists {
			seen[trimmed] = struct{}{}
			unique = append(unique, trimmed)
		}
	}
	return unique
}

func printSummary(interrupted bool) {
	elapsed := time.Since(startTime).Round(time.Second)

	// Use logger for summary output
	log.Println("\n═══════════════════ PROCESSING SUMMARY ═══════════════════")
	status := "Processing complete"
	if interrupted {
		status = "PROCESSING INTERRUPTED"
	}
	log.Printf(" Status:               %s", status)
	log.Println("────────────────────────────────────────────────────────────")
	log.Printf(" Total items submitted:   %d", totalItems)
	log.Printf(" Successfully processed:  %d", processedCount.Load())
	log.Printf(" Skipped (archived):      %d", skippedCount.Load())
	log.Printf(" Errors/Cancelled:        %d", errorCount.Load())
	log.Printf(" Total duration:          %s", elapsed)
	log.Println("════════════════════════════════════════════════════════════")
}
