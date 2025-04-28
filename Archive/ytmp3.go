package main

import (
	"bufio"
	"fmt"
	"io" // Required for io.Copy
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time" // Required for timing
)

const (
	outputBaseDir   = "."
	archiveFilename = "ytmp3_processed_archive.txt"
)

// --- Global State ---
var (
	processedCount atomic.Uint64
	skippedCount   atomic.Uint64
	errorCount     atomic.Uint64
	itemsDone      atomic.Uint64 // Counter for completed items (success or error)
)
var archiveMutex sync.Mutex
var processedArchive = make(map[string]struct{})
var totalItems int      // Total items to process
var startTime time.Time // Script start time

// --- Main Execution ---

func main() {
	log.SetFlags(0)
	startTime = time.Now() // Record start time

	exePath, err := os.Executable()
	if err != nil {
		log.Fatalf("FATAL: Could not determine executable path: %v", err)
	}
	scriptDir := filepath.Dir(exePath)
	archivePath := filepath.Join(scriptDir, archiveFilename)
	log.Printf("Using archive file: %s", archivePath)

	err = loadArchive(archivePath)
	if err != nil {
		log.Fatalf("FATAL: Failed to load or create archive file '%s': %v", archivePath, err)
	}

	args := os.Args[1:]
	totalItems = len(args) // Set total items count
	if totalItems == 0 {
		printUsage()
		os.Exit(1)
	}

	log.Printf("Starting processing for %d items...", totalItems)

	var wg sync.WaitGroup
	for i, videoIdentifier := range args {
		identifier := videoIdentifier // Local copy for goroutine
		itemNumber := i + 1           // User-friendly 1-based index

		archiveMutex.Lock()
		_, exists := processedArchive[identifier]
		archiveMutex.Unlock()

		if exists {
			log.Printf("Skipping [%d/%d]: '%s' (already in archive)", itemNumber, totalItems, identifier)
			skippedCount.Add(1)
			itemsDone.Add(1) // Count skipped items as "done" for progress tracking
			continue
		}

		wg.Add(1)
		go processVideo(identifier, archivePath, &wg, itemNumber)
	}

	wg.Wait()

	elapsedTime := time.Since(startTime)
	log.Println("==================================================")
	log.Println("Processing Summary:")
	log.Printf("  Total Items Submitted: %d", totalItems)
	log.Printf("  Successfully processed: %d", processedCount.Load())
	log.Printf("  Skipped (already in archive): %d", skippedCount.Load())
	log.Printf("  Errors: %d", errorCount.Load())
	log.Printf("  Total items completed: %d", itemsDone.Load()) // Should match totalItems if all ran
	log.Printf("  Archive file: %s", archivePath)
	log.Printf("  Total processing time: %s", elapsedTime.Round(time.Second)) // Rounded duration
	log.Println("==================================================")

	if errorCount.Load() > 0 {
		os.Exit(1)
	}
	os.Exit(0)
}

func printUsage() {
	// (Usage message remains the same as the previous version)
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

func loadArchive(archivePath string) error {
	// (loadArchive function remains the same)
	archiveMutex.Lock()
	defer archiveMutex.Unlock()
	processedArchive = make(map[string]struct{})
	file, err := os.OpenFile(archivePath, os.O_RDONLY|os.O_CREATE, 0644)
	if err != nil {
		return fmt.Errorf("failed to open or create archive file '%s': %w", archivePath, err)
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	count := 0
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line != "" {
			processedArchive[line] = struct{}{}
			count++
		}
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("error reading archive file '%s': %w", archivePath, err)
	}
	log.Printf("Loaded %d entries from archive.", count)
	return nil
}

func appendToArchive(archivePath string, identifier string) error {
	// (appendToArchive function remains the same)
	archiveMutex.Lock()
	defer archiveMutex.Unlock()
	file, err := os.OpenFile(archivePath, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0644)
	if err != nil {
		return fmt.Errorf("failed to open archive file '%s' for appending: %w", archivePath, err)
	}
	defer file.Close()
	if _, err := fmt.Fprintln(file, identifier); err != nil {
		return fmt.Errorf("failed to write identifier '%s' to archive file '%s': %w", identifier, archivePath, err)
	}
	processedArchive[identifier] = struct{}{}
	return nil
}

// processVideo now includes itemNumber for progress reporting
func processVideo(videoIdentifier string, archivePath string, wg *sync.WaitGroup, itemNumber int) {
	// Increment itemsDone counter when this function finishes, regardless of outcome
	defer itemsDone.Add(1)
	// Signal WaitGroup that this goroutine is done when function exits
	defer wg.Done()

	// --- Print Start Message with Progress ---
	// Use Load to safely read atomic counters
	currentItemNum := itemsDone.Load() + 1 // +1 because this one hasn't finished yet
	log.Printf("--- Processing [%d/%d]: %s ---", currentItemNum, totalItems, videoIdentifier)

	outputTemplateChapter := fmt.Sprintf("chapter:%s/%%(title)s/%%(section_title)s - %%(title)s.%%(ext)s", outputBaseDir)

	cmd := exec.Command("yt-dlp",
		"--color", "always", // Attempt to force color output
		// "--no-progress",   // Alternative: uncomment if progress bars are too messy
		"--default-search", "ytsearch",
		"-i",
		"-f", "bestvideo*+bestaudio/best",
		"--extract-audio",
		"--audio-format", "mp3",
		"--audio-quality", "0",
		"--split-chapters",
		"--embed-thumbnail",
		"-o", outputTemplateChapter,
		videoIdentifier,
	)

	// --- Setup Live Output Streaming (Directly to os.Stdout/os.Stderr) ---
	stdoutPipe, err := cmd.StdoutPipe()
	if err != nil {
		log.Printf("ERROR [%d/%d] %s: Failed to get stdout pipe: %v", itemNumber, totalItems, videoIdentifier, err)
		errorCount.Add(1)
		return
	}
	stderrPipe, err := cmd.StderrPipe()
	if err != nil {
		log.Printf("ERROR [%d/%d] %s: Failed to get stderr pipe: %v", itemNumber, totalItems, videoIdentifier, err)
		errorCount.Add(1)
		return
	}

	if err := cmd.Start(); err != nil {
		log.Printf("ERROR [%d/%d] %s: Failed to start yt-dlp command: %v", itemNumber, totalItems, videoIdentifier, err)
		errorCount.Add(1)
		return
	}
	// We won't log PID here to keep output cleaner, focusing on yt-dlp's output

	// Use io.Copy for potentially more efficient streaming directly to outputs
	var streamWg sync.WaitGroup
	streamWg.Add(2)

	go func() {
		defer streamWg.Done()
		// Copy yt-dlp's stdout directly to our program's stdout
		_, copyErr := io.Copy(os.Stdout, stdoutPipe)
		if copyErr != nil && copyErr != io.EOF {
			// Log errors copying the stream, but might be expected on close
			// log.Printf("WARN [%d/%d] %s: Error copying yt-dlp stdout: %v", itemNumber, totalItems, videoIdentifier, copyErr)
		}
	}()

	go func() {
		defer streamWg.Done()
		// Copy yt-dlp's stderr directly to our program's stderr
		_, copyErr := io.Copy(os.Stderr, stderrPipe)
		if copyErr != nil && copyErr != io.EOF {
			// log.Printf("WARN [%d/%d] %s: Error copying yt-dlp stderr: %v", itemNumber, totalItems, videoIdentifier, copyErr)
		}
	}()

	streamWg.Wait() // Wait for both streams to finish copying

	err = cmd.Wait()

	// --- Handle Command Result & Print End Message ---
	doneCount := itemsDone.Load() + 1 // Get the count *after* this item is logically done
	if err != nil {
		log.Printf("--- ERROR Finished [%d/%d]: %s (yt-dlp error: %v) ---", doneCount, totalItems, videoIdentifier, err)
		errorCount.Add(1)
		// Don't return here, let itemsDone be incremented by the defer
	} else {
		// Attempt to archive only on success
		archiveErr := appendToArchive(archivePath, videoIdentifier)
		if archiveErr != nil {
			log.Printf("--- ERROR Finished [%d/%d]: %s (Failed to update archive: %v) ---", doneCount, totalItems, videoIdentifier, archiveErr)
			errorCount.Add(1) // Count archive failure as an error
		} else {
			log.Printf("--- Success Finished [%d/%d]: %s (Archived) ---", doneCount, totalItems, videoIdentifier)
			processedCount.Add(1)
		}
	}
	// itemsDone is incremented by the top-level defer in this function
}
