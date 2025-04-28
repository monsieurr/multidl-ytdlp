package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
)

const (
	// outputBaseDir defines the root directory relative to the script's execution path
	// where the video-specific folders will be created.
	outputBaseDir = "." // Current directory

	// archiveFilename is the name of the file used to track processed video identifiers.
	archiveFilename = "ytmp3_processed_archive.txt"
)

// --- Global State ---

// Counters for the final summary report. Use atomic operations for safe concurrent updates.
var (
	processedCount atomic.Uint64
	skippedCount   atomic.Uint64
	errorCount     atomic.Uint64
)

// archiveMutex protects concurrent access (reads and writes) to the archive file
// and the in-memory representation (processedArchive map).
var archiveMutex sync.Mutex

// processedArchive holds the set of identifiers found in the archive file for fast lookups.
// Using struct{} is memory-efficient for set-like behavior (we only care about key existence).
var processedArchive = make(map[string]struct{})

// --- Main Execution ---

func main() {
	// Configure logger for cleaner output (no timestamps/prefixes by default)
	log.SetFlags(0)

	// --- Determine Archive File Path ---
	// Get the directory where the executable is located.
	exePath, err := os.Executable()
	if err != nil {
		// If we can't find the executable path, we can't reliably find the archive. Fatal error.
		log.Fatalf("FATAL: Could not determine executable path: %v", err)
	}
	scriptDir := filepath.Dir(exePath)
	// Construct the full path to the archive file.
	archivePath := filepath.Join(scriptDir, archiveFilename)
	log.Printf("Using archive file: %s", archivePath)

	// --- Load or Create Archive File ---
	// Load existing entries into the processedArchive map. Create the file if it doesn't exist.
	err = loadArchive(archivePath)
	if err != nil {
		log.Fatalf("FATAL: Failed to load or create archive file '%s': %v", archivePath, err)
	}

	// --- Process Command Line Arguments ---
	args := os.Args[1:] // os.Args[0] is the program name, we want the subsequent arguments.
	if len(args) == 0 {
		// No arguments provided, print usage instructions and exit.
		printUsage()
		os.Exit(1)
	}

	// --- Concurrently Process Video Identifiers ---
	var wg sync.WaitGroup // Use a WaitGroup to wait for all processing goroutines to complete.

	log.Println("Starting processing...")

	for _, videoIdentifier := range args {
		// Make a local copy of the identifier for the goroutine.
		// This avoids the common loop variable capture issue in Go, ensuring each goroutine
		// gets the correct identifier it's supposed to process.
		identifier := videoIdentifier

		// Check if this identifier is already in our loaded archive (fast map lookup).
		archiveMutex.Lock() // Lock needed for reading the shared map, just in case loadArchive runs concurrently (future proofing)
		_, exists := processedArchive[identifier]
		archiveMutex.Unlock()

		if exists {
			log.Printf("Skipping: '%s' is already in the archive.", identifier)
			skippedCount.Add(1) // Safely increment the skipped counter.
			continue            // Move to the next argument.
		}

		// If not skipped, increment the WaitGroup counter and launch a goroutine.
		wg.Add(1)
		go processVideo(identifier, archivePath, &wg)
	}

	// --- Wait for Completion and Summarize ---
	wg.Wait() // Block execution until all goroutines launched above have called wg.Done().

	log.Println("==================================================")
	log.Println("Processing Summary:")
	log.Printf("  Successfully processed: %d", processedCount.Load())
	log.Printf("  Skipped (already in archive): %d", skippedCount.Load())
	log.Printf("  Errors: %d", errorCount.Load())
	log.Printf("Archive file: %s", archivePath)
	log.Println("==================================================")

	// Exit with a non-zero status code if any errors occurred during processing.
	if errorCount.Load() > 0 {
		os.Exit(1)
	}
	os.Exit(0) // Exit successfully.
}

// printUsage displays help information about how to run the script.
func printUsage() {
	appName := filepath.Base(os.Args[0]) // Get the name of the executable.
	fmt.Fprintf(os.Stderr, "Usage: %s <URL_or_ID_1> [URL_or_ID_2] ...\n\n", appName)
	fmt.Fprintf(os.Stderr, "Downloads YouTube video audio as chaptered MP3s into a folder named after the video title.\n")
	fmt.Fprintf(os.Stderr, "Accepts full YouTube URLs, video IDs, playlist URLs/IDs, or search terms.\n")
	fmt.Fprintf(os.Stderr, "Uses archive file '%s' located in the same directory as the executable to avoid reprocessing videos.\n\n", archiveFilename)
	fmt.Fprintf(os.Stderr, "Important:\n")
	fmt.Fprintf(os.Stderr, " - Requires 'yt-dlp' to be installed and accessible in your system's PATH.\n")
	fmt.Fprintf(os.Stderr, " - Remember to quote arguments containing special shell characters (like '&', '?', '=')\n")
	fmt.Fprintf(os.Stderr, "   Example: %s 'https://www.youtube.com/watch?v=dQw4w9WgXcQ&list=...' 'Some Video ID'\n", appName)
}

// loadArchive reads the archive file into the global processedArchive map.
// It creates the file if it doesn't exist. Uses a mutex for safety.
func loadArchive(archivePath string) error {
	archiveMutex.Lock()         // Lock before accessing shared resources (map and potentially file)
	defer archiveMutex.Unlock() // Ensure unlock happens even if errors occur later

	// Clear the map to ensure a fresh load (important if this were called multiple times)
	processedArchive = make(map[string]struct{})

	// Open the file for reading, create it if it doesn't exist.
	file, err := os.OpenFile(archivePath, os.O_RDONLY|os.O_CREATE, 0644)
	if err != nil {
		return fmt.Errorf("failed to open or create archive file '%s': %w", archivePath, err)
	}
	defer file.Close() // Ensure the file is closed when the function returns.

	// Read the file line by line.
	scanner := bufio.NewScanner(file)
	count := 0
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text()) // Remove leading/trailing whitespace.
		if line != "" {
			processedArchive[line] = struct{}{} // Add the non-empty line to the map.
			count++
		}
	}

	// Check for errors during scanning (e.g., read errors).
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("error reading archive file '%s': %w", archivePath, err)
	}

	log.Printf("Loaded %d entries from archive.", count)
	return nil // Success
}

// appendToArchive adds a given identifier to the archive file and the in-memory map.
// It uses a mutex to prevent race conditions during concurrent writes/reads.
func appendToArchive(archivePath string, identifier string) error {
	archiveMutex.Lock()         // Acquire lock before accessing shared resources.
	defer archiveMutex.Unlock() // Ensure lock is released.

	// Open the file in append mode, create if it doesn't exist.
	file, err := os.OpenFile(archivePath, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0644)
	if err != nil {
		return fmt.Errorf("failed to open archive file '%s' for appending: %w", archivePath, err)
	}
	defer file.Close()

	// Write the identifier followed by a newline character.
	if _, err := fmt.Fprintln(file, identifier); err != nil {
		return fmt.Errorf("failed to write identifier '%s' to archive file '%s': %w", identifier, archivePath, err)
	}

	// Also update the in-memory map to reflect the change immediately.
	// This prevents another goroutine from potentially reprocessing the same identifier
	// within the same run if it checks the map *after* this goroutine finished the file write
	// but before the main loop check could re-evaluate.
	processedArchive[identifier] = struct{}{}

	return nil // Success
}

// processVideo handles the core logic for processing a single video identifier.
// It calls yt-dlp, streams its output, and updates the archive upon success.
// Designed to be run concurrently in a goroutine.
func processVideo(videoIdentifier string, archivePath string, wg *sync.WaitGroup) {
	// Ensure wg.Done() is called when this function exits, signaling completion to the main routine.
	defer wg.Done()

	log.Printf("--------------------------------------------------")
	log.Printf("Processing: %s", videoIdentifier)

	// Define the yt-dlp output template for chapter splitting.
	// %% is used to escape '%' characters so they are passed literally to yt-dlp,
	// which then substitutes its own variables like %(title)s.
	outputTemplateChapter := fmt.Sprintf("chapter:%s/%%(title)s/%%(section_title)s - %%(title)s.%%(ext)s", outputBaseDir)

	// Prepare the external command execution for yt-dlp.
	cmd := exec.Command("yt-dlp",
		// Flags for yt-dlp:
		"--default-search", "ytsearch", // Treat ambiguous input as a YouTube search term
		"-i",                              // Ignore errors (e.g., unavailable video in a playlist)
		"-f", "bestvideo*+bestaudio/best", // Select best video/audio, prioritizing combined formats
		"--extract-audio",       // Extract audio track
		"--audio-format", "mp3", // Convert audio to MP3
		"--audio-quality", "0", // Set MP3 quality (0 is typically best VBR)
		"--split-chapters",  // Split output file by chapter markers
		"--embed-thumbnail", // Embed thumbnail into the audio file
		// "--no-progress",        // Uncomment this if yt-dlp's progress bars interfere with concurrent output logging
		"-o", outputTemplateChapter, // Output filename template for chapters
		videoIdentifier, // The actual URL, ID, or search term to process
	)

	// --- Setup Live Output Streaming ---
	// Get pipes for reading the stdout and stderr streams of the command.
	stdoutPipe, err := cmd.StdoutPipe()
	if err != nil {
		log.Printf("ERROR [%s]: Failed to get stdout pipe: %v", videoIdentifier, err)
		errorCount.Add(1) // Count this setup failure as an error.
		return            // Cannot proceed without the pipe.
	}
	stderrPipe, err := cmd.StderrPipe()
	if err != nil {
		log.Printf("ERROR [%s]: Failed to get stderr pipe: %v", videoIdentifier, err)
		errorCount.Add(1)
		return
	}

	// Start the command asynchronously. This doesn't block.
	if err := cmd.Start(); err != nil {
		log.Printf("ERROR [%s]: Failed to start yt-dlp command: %v", videoIdentifier, err)
		errorCount.Add(1)
		return
	}
	log.Printf("INFO [%s]: yt-dlp process started (PID: %d).", videoIdentifier, cmd.Process.Pid)

	// Use a separate WaitGroup to manage the goroutines that read output pipes.
	var outputWg sync.WaitGroup
	outputWg.Add(2) // We need to wait for both stdout and stderr readers.

	// Goroutine to read and print stdout.
	go func() {
		defer outputWg.Done() // Signal completion when this goroutine finishes.
		scanner := bufio.NewScanner(stdoutPipe)
		// Read line by line until the pipe is closed.
		for scanner.Scan() {
			// Log each line from yt-dlp's stdout, prefixing for clarity.
			log.Printf("[yt-dlp stdout - %s]: %s", videoIdentifier, scanner.Text())
		}
		// Check for errors during scanning (other than io.EOF).
		if err := scanner.Err(); err != nil {
			log.Printf("WARN [%s]: Error reading yt-dlp stdout pipe: %v", videoIdentifier, err)
		}
	}()

	// Goroutine to read and print stderr.
	go func() {
		defer outputWg.Done() // Signal completion.
		scanner := bufio.NewScanner(stderrPipe)
		for scanner.Scan() {
			// Log each line from yt-dlp's stderr.
			log.Printf("[yt-dlp stderr - %s]: %s", videoIdentifier, scanner.Text())
		}
		if err := scanner.Err(); err != nil {
			log.Printf("WARN [%s]: Error reading yt-dlp stderr pipe: %v", videoIdentifier, err)
		}
	}()

	// Wait for both the stdout and stderr reading goroutines to finish.
	// This ensures all output has been captured before we proceed.
	outputWg.Wait()

	// Wait for the yt-dlp command itself to complete and capture its exit status.
	// This blocks until the process terminates.
	err = cmd.Wait()

	// --- Handle Command Result ---
	if err != nil {
		// yt-dlp exited with a non-zero status code. Log this as an error.
		// Note: Even with '-i', cmd.Wait() might return an error if the core process fails.
		log.Printf("ERROR [%s]: yt-dlp command finished with error: %v", videoIdentifier, err)
		errorCount.Add(1) // Increment the global error counter.
		return            // Do not attempt to archive if the command failed.
	}

	// Command completed successfully (exit code 0).
	log.Printf("INFO [%s]: yt-dlp process finished successfully.", videoIdentifier)

	// --- Update Archive File Safely ---
	// Now that yt-dlp succeeded, add the identifier to the archive file and map.
	err = appendToArchive(archivePath, videoIdentifier)
	if err != nil {
		// If appending to the archive fails, log it as an error.
		log.Printf("ERROR [%s]: Failed to update archive file '%s' after successful processing: %v", videoIdentifier, archivePath, err)
		errorCount.Add(1) // Count failure to update archive as an error.
	} else {
		// Successfully processed and archived.
		log.Printf("Added '%s' to archive.", videoIdentifier)
		processedCount.Add(1) // Increment the global success counter.
	}
	// No need to update the map here again, appendToArchive already does it atomically.
}
