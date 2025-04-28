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
	outputBaseDir   = "." // Base directory for output folders
	archiveFilename = "ytmp3_processed_archive.txt"
)

// Global counters for summary (use atomic operations for concurrent updates)
var (
	processedCount atomic.Uint64
	skippedCount   atomic.Uint64
	errorCount     atomic.Uint64
)

// Mutex to protect concurrent writes to the archive file
var archiveMutex sync.Mutex

// Set to store processed video identifiers for quick lookup
var processedArchive = make(map[string]struct{}) // Using struct{} as value is memory efficient for sets

func main() {
	log.SetFlags(0) // Remove timestamp prefixes from log messages

	// --- Determine Archive File Path ---
	exePath, err := os.Executable()
	if err != nil {
		log.Fatalf("FATAL: Could not determine executable path: %v", err)
	}
	scriptDir := filepath.Dir(exePath)
	archivePath := filepath.Join(scriptDir, archiveFilename)
	log.Printf("Using archive file: %s", archivePath)

	// --- Load or Create Archive ---
	err = loadArchive(archivePath)
	if err != nil {
		log.Fatalf("FATAL: Failed to load or create archive file: %v", err)
	}

	// --- Process Command Line Arguments ---
	args := os.Args[1:] // Exclude the program name itself
	if len(args) == 0 {
		appName := filepath.Base(os.Args[0])
		fmt.Fprintf(os.Stderr, "Usage: %s <URL_or_ID_1> [URL_or_ID_2] ...\n", appName)
		fmt.Fprintf(os.Stderr, "Downloads YouTube video audio as chaptered MP3s into a folder named after the video title.\n")
		fmt.Fprintf(os.Stderr, "Accepts full YouTube URLs or just the video ID.\n")
		fmt.Fprintf(os.Stderr, "Uses archive file '%s' in script directory to avoid reprocessing.\n", archiveFilename)
		os.Exit(1)
	}

	// --- Concurrently Process Videos ---
	var wg sync.WaitGroup // Use a WaitGroup to wait for all goroutines to finish

	log.Println("Starting processing...")

	for _, videoIdentifier := range args {
		// Check if already processed (read from the map is safe concurrently)
		if _, exists := processedArchive[videoIdentifier]; exists {
			log.Printf("Skipping: '%s' is already in the archive (%s).", videoIdentifier, archiveFilename)
			skippedCount.Add(1)
			continue // Skip to the next identifier
		}

		// Launch a goroutine for each video identifier to process
		wg.Add(1) // Increment the WaitGroup counter
		go processVideo(videoIdentifier, archivePath, &wg)
	}

	// --- Wait and Summarize ---
	wg.Wait() // Block until all goroutines have called wg.Done()

	log.Println("==================================================")
	log.Println("Processing Summary:")
	log.Printf("  Successfully processed: %d\n", processedCount.Load())
	log.Printf("  Skipped (already in archive): %d\n", skippedCount.Load())
	log.Printf("  Errors: %d\n", errorCount.Load())
	log.Printf("Archive file: %s\n", archivePath)
	log.Println("==================================================")

	// Exit with non-zero status if any errors occurred
	if errorCount.Load() > 0 {
		os.Exit(1)
	}
	os.Exit(0)
}

// loadArchive reads the existing archive file into the processedArchive map.
// If the file doesn't exist, it creates an empty one.
func loadArchive(archivePath string) error {
	file, err := os.OpenFile(archivePath, os.O_RDONLY|os.O_CREATE, 0644)
	if err != nil {
		return fmt.Errorf("failed to open or create archive file '%s': %w", archivePath, err)
	}
	defer file.Close() // Ensure file is closed

	scanner := bufio.NewScanner(file)
	count := 0
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line != "" {
			processedArchive[line] = struct{}{} // Add to the set
			count++
		}
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("error reading archive file '%s': %w", archivePath, err)
	}
	log.Printf("Loaded %d entries from archive.", count)
	return nil
}

// processVideo handles the processing for a single video identifier.
// It's designed to be run in a separate goroutine.
func processVideo(videoIdentifier string, archivePath string, wg *sync.WaitGroup) {
	defer wg.Done() // Decrement the WaitGroup counter when this function exits

	log.Printf("--------------------------------------------------")
	log.Printf("Processing: %s", videoIdentifier)

	// Define the chapter output template including the directory
	// NOTE: yt-dlp handles the variable substitution (%(title)s etc.)
	outputTemplateChapter := fmt.Sprintf("chapter:%s/%%(title)s/%%(section_title)s - %%(title)s.%%(ext)s", outputBaseDir)

	// Prepare the yt-dlp command
	// Use CombinedOutput to capture both stdout and stderr for better debugging
	cmd := exec.Command("yt-dlp",
		"-i",                              // Ignore errors
		"-f", "bestvideo*+bestaudio/best", // Format selection
		"--extract-audio",
		"--audio-format", "mp3",
		"--audio-quality", "0", // Best quality MP3
		"--split-chapters",
		"--embed-thumbnail",
		"-o", outputTemplateChapter, // Output template
		videoIdentifier, // The URL or ID
	)

	// Run yt-dlp and capture output/error
	output, err := cmd.CombinedOutput()

	// --- Handle Command Result ---
	if err != nil {
		log.Printf("ERROR processing '%s': yt-dlp failed (Exit Code potentially non-zero): %v", videoIdentifier, err)
		log.Printf("yt-dlp output for '%s':\n%s", videoIdentifier, string(output)) // Log output for debugging
		errorCount.Add(1)
		return // Stop processing this identifier
	}

	// Command succeeded
	log.Printf("Successfully processed: %s", videoIdentifier)
	// log.Printf("yt-dlp output for '%s':\n%s", videoIdentifier, string(output)) // Optional: Log successful output too

	// --- Update Archive File Safely ---
	err = appendToArchive(archivePath, videoIdentifier)
	if err != nil {
		log.Printf("ERROR: Failed to update archive file '%s' for identifier '%s': %v", archivePath, videoIdentifier, err)
		errorCount.Add(1)
		// Continue, but count as an error because the archive wasn't updated
	} else {
		log.Printf("Added '%s' to archive.", videoIdentifier)
		processedCount.Add(1)
		// Also add to the in-memory map to prevent potential reprocessing
		// within the same run if the same ID was provided multiple times.
		// Lock needed here if we modify the shared map *after* initial load.
		// However, since we check *before* starting the goroutine, adding here
		// is mostly for consistency within a single long run if needed, but
		// the primary lock is on the file write itself.
		// archiveMutex.Lock()
		// processedArchive[videoIdentifier] = struct{}{}
		// archiveMutex.Unlock()
	}
}

// appendToArchive safely appends a line to the archive file using a mutex.
func appendToArchive(archivePath string, identifier string) error {
	archiveMutex.Lock()         // Acquire lock before accessing the file
	defer archiveMutex.Unlock() // Ensure lock is released

	file, err := os.OpenFile(archivePath, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0644)
	if err != nil {
		return fmt.Errorf("failed to open archive file '%s' for appending: %w", archivePath, err)
	}
	defer file.Close()

	// Append the identifier followed by a newline
	if _, err := fmt.Fprintln(file, identifier); err != nil {
		return fmt.Errorf("failed to write identifier '%s' to archive file '%s': %w", identifier, archivePath, err)
	}

	return nil
}
