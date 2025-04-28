package main

import (
	"bufio"
	"context"
	"fmt"
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
	outputBaseDir   = "."
	archiveFilename = "ytmp3_processed_archive.txt"
)

type SafeSet struct {
	sync.Mutex
	m map[string]struct{}
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
	outputMutex      sync.Mutex
)

func main() {
	log.SetFlags(0)
	startTime = time.Now()

	signal.Notify(shutdownSignal, syscall.SIGINT, syscall.SIGTERM)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	exePath, err := os.Executable()
	if err != nil {
		log.Fatalf("FATAL: Could not determine executable path: %v", err)
	}
	archivePath := filepath.Join(filepath.Dir(exePath), archiveFilename)

	if err := initArchive(archivePath); err != nil {
		log.Fatalf("FATAL: Archive initialization failed: %v", err)
	}

	args := deduplicateArgs(os.Args[1:])
	totalItems = len(args)
	if totalItems == 0 {
		printUsage()
		os.Exit(1)
	}

	fmt.Printf("Starting processing for %d items at %s\n\n", totalItems, startTime.Format("15:04:05"))

	var wg sync.WaitGroup
	results := make(chan processingResult, totalItems)

	for i, identifier := range args {
		wg.Add(1)
		go processVideo(ctx, &wg, identifier, archivePath, i+1, results)
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	var interrupted bool
	defer func() { printSummary(interrupted) }()

	for result := range results {
		handleProcessingResult(result, archivePath)
	}
}

func initArchive(path string) error {
	processedArchive = SafeSet{m: make(map[string]struct{})}

	file, err := os.OpenFile(path, os.O_RDONLY|os.O_CREATE, 0644)
	if err != nil {
		return fmt.Errorf("failed to open archive: %w", err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line != "" {
			processedArchive.m[line] = struct{}{}
		}
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("archive read error: %w", err)
	}
	return nil
}

func processVideo(ctx context.Context, wg *sync.WaitGroup, identifier, archivePath string, itemNumber int, results chan<- processingResult) {
	defer wg.Done()

	result := processingResult{
		Identifier: identifier,
		ItemNumber: itemNumber,
		StartTime:  time.Now(),
	}

	defer func() {
		result.Duration = time.Since(result.StartTime)
		results <- result
	}()

	outputMutex.Lock()
	fmt.Printf("\n╔════ ITEM %d/%d ════════════════════════════════\n", itemNumber, totalItems)
	fmt.Printf("║ URL: %s\n", identifier)
	fmt.Printf("║ Start: %s\n", result.StartTime.Format("15:04:05"))
	fmt.Println("╚═══════════════════════════════════════════════")
	outputMutex.Unlock()

	if !markPending(identifier) {
		result.Error = fmt.Errorf("duplicate in progress")
		return
	}
	defer unmarkPending(identifier)

	processedArchive.Lock()
	_, exists := processedArchive.m[identifier]
	processedArchive.Unlock()

	if exists {
		result.Error = fmt.Errorf("skipped (archived)")
		return
	}

	cmd := exec.CommandContext(ctx, "yt-dlp",
		"--color", "always",
		"--progress",
		"--newline",
		"--progress-template", "[download] %(progress._percent_str)s of %(progress._total_bytes_str)s at %(progress._speed_str)s ETA %(progress._eta_str)s",
		"--console-title",
		"--extract-audio",
		"--audio-format", "mp3",
		"--audio-quality", "0",
		"--split-chapters",
		"--embed-thumbnail",
		"-o", fmt.Sprintf("chapter:%s/%%(title)s/%%(section_title)s - %%(title)s.%%(ext)s", outputBaseDir),
		identifier,
	)

	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Run(); err != nil {
		result.Error = fmt.Errorf("yt-dlp error: %w", err)
		return
	}

	if err := appendToArchive(archivePath, identifier); err != nil {
		result.ArchiveErr = err
	}
}

func markPending(identifier string) bool {
	pending.Lock()
	defer pending.Unlock()

	if pending.m == nil {
		pending.m = make(map[string]struct{})
	}

	if _, exists := pending.m[identifier]; exists {
		return false
	}

	pending.m[identifier] = struct{}{}
	return true
}

func unmarkPending(identifier string) {
	pending.Lock()
	defer pending.Unlock()
	delete(pending.m, identifier)
}

func appendToArchive(path string, identifier string) error {
	processedArchive.Lock()
	defer processedArchive.Unlock()

	file, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0644)
	if err != nil {
		return fmt.Errorf("archive open failed: %w", err)
	}
	defer file.Close()

	if _, err := fmt.Fprintln(file, identifier); err != nil {
		return fmt.Errorf("archive write failed: %w", err)
	}

	processedArchive.m[identifier] = struct{}{}
	return nil
}

func handleProcessingResult(result processingResult, archivePath string) {
	baseMsg := fmt.Sprintf("[%d] %s (%s)",
		result.ItemNumber, result.Identifier, result.Duration.Round(time.Second))

	switch {
	case result.ArchiveErr != nil:
		log.Printf("%s - Archive Error: %v", baseMsg, result.ArchiveErr)
		errorCount.Add(1)
	case result.Error != nil:
		if strings.Contains(result.Error.Error(), "skipped") {
			log.Printf("%s - Skipped", baseMsg)
			skippedCount.Add(1)
		} else {
			log.Printf("%s - Failed: %v", baseMsg, result.Error)
			errorCount.Add(1)
		}
	default:
		log.Printf("%s - Success", baseMsg)
		processedCount.Add(1)
	}
}

func deduplicateArgs(args []string) []string {
	seen := make(map[string]struct{})
	unique := make([]string, 0, len(args))
	for _, arg := range args {
		if _, exists := seen[arg]; !exists {
			seen[arg] = struct{}{}
			unique = append(unique, arg)
		}
	}
	return unique
}

func printSummary(interrupted bool) {
	elapsed := time.Since(startTime)

	fmt.Println("\n═══════════════════════════════════════════════")
	if interrupted {
		fmt.Println(" PROCESSING INTERRUPTED")
	} else {
		fmt.Println(" PROCESSING COMPLETE")
	}

	fmt.Println("═══════════════════════════════════════════════")
	fmt.Printf("  Total items submitted:   %d\n", totalItems)
	fmt.Printf("  Successfully processed:  %d\n", processedCount.Load())
	fmt.Printf("  Skipped (archived):      %d\n", skippedCount.Load())
	fmt.Printf("  Errors:                  %d\n", errorCount.Load())
	fmt.Printf("  Total duration:          %s\n", elapsed.Round(time.Second))
	fmt.Println("═══════════════════════════════════════════════")
}

func printUsage() {
	fmt.Printf(`
Usage: %s [URL/ID...]

Process YouTube videos/playlists and save as chaptered MP3s
Uses archive file: %s

Arguments:
  Accepts multiple YouTube URLs/IDs, playlist links, or search terms
`, filepath.Base(os.Args[0]), archiveFilename)
}
