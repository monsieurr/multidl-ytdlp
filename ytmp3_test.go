package main

import (
	"bufio"
	"bytes" // Required for capturing command output
	"fmt"
	"io"
	// "io/fs" // Removed: Not explicitly needed for os.ReadDir
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	outputBaseDir = "."
	archiveFilename = "ytmp3_processed_archive.txt"
)

// Define potential thumbnail extensions yt-dlp might save
var thumbnailExtensionsList = []string{".jpg", ".jpeg", ".png", ".webp"}

// --- Global State ---
var (
	processedCount atomic.Uint64
	skippedCount   atomic.Uint64
	errorCount     atomic.Uint64
	itemsDone      atomic.Uint64
)
var archiveMutex sync.Mutex
var processedArchive = make(map[string]struct{})
var totalItems int
var startTime time.Time

// Helper function to sanitize filenames/directory names (basic example)
func sanitizeFilename(name string) string {
	// Replace characters invalid in Windows/Linux filenames. Adjust as needed.
	replacer := strings.NewReplacer(
		`/`, "_", `\`, "_", `:`, "_", `*`, "_", `?`, "_", `"`, "'", `<`, "_", `>`, "_", `|`, "_",
	)
	sanitized := replacer.Replace(name)
	// Trim leading/trailing spaces/dots which can cause issues
	sanitized = strings.Trim(sanitized, ". ")
	return sanitized
}

// Helper function to get video title using yt-dlp
func getVideoTitle(videoIdentifier string) (string, error) {
	cmd := exec.Command("yt-dlp",
		"--print", "%(title)s",
		"--skip-download",
		"--no-warnings",
		"--default-search", "ytsearch",
		videoIdentifier,
	)

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	err := cmd.Run()
	if err != nil {
		title := strings.TrimSpace(stdout.String())
		if title != "" {
			log.Printf("WARN: yt-dlp exited with error getting title, but title was found: %v. Using title: %s", err, title)
			return title, nil
		}
		return "", fmt.Errorf("failed to get video title: %w\nstderr: %s", err, stderr.String())
	}

	title := strings.TrimSpace(stdout.String())
	if title == "" {
		return "", fmt.Errorf("failed to get video title: yt-dlp returned empty title")
	}
	return title, nil
}

// --- Main Execution ---

func main() {
	log.SetFlags(0)
	startTime = time.Now()

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
	totalItems = len(args)
	if totalItems == 0 {
		printUsage()
		os.Exit(1)
	}

	if _, err := exec.LookPath("ffmpeg"); err != nil {
		log.Printf("WARNING: 'ffmpeg' command not found in PATH. Thumbnail embedding in chapter files will fail.")
	}

	log.Printf("Starting processing for %d items...", totalItems)

	var wg sync.WaitGroup
	for i, videoIdentifier := range args {
		identifier := videoIdentifier
		itemNumber := i + 1

		archiveMutex.Lock()
		_, exists := processedArchive[identifier]
		archiveMutex.Unlock()

		if exists {
			log.Printf("Skipping [%d/%d]: '%s' (already in archive)", itemNumber, totalItems, identifier)
			skippedCount.Add(1)
			itemsDone.Add(1)
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
	log.Printf("  Successfully processed (yt-dlp OK & Archived): %d", processedCount.Load())
	log.Printf("  Skipped (already in archive): %d", skippedCount.Load())
	log.Printf("  Errors (yt-dlp failure or archive error): %d", errorCount.Load())
	log.Printf("  Total items completed: %d", itemsDone.Load())
	log.Printf("  Archive file: %s", archivePath)
	log.Printf("  Total processing time: %s", elapsedTime.Round(time.Second))
	log.Println("==================================================")

	if errorCount.Load() > 0 {
		os.Exit(1)
	}
	os.Exit(0)
}

func printUsage() {
	appName := filepath.Base(os.Args[0])
	fmt.Fprintf(os.Stderr, "Usage: %s <URL_or_ID_1> [URL_or_ID_2] ...\n\n", appName)
	fmt.Fprintf(os.Stderr, "Downloads YouTube video audio as chaptered MP3s into a folder named after the video title,\n")
	fmt.Fprintf(os.Stderr, "and embeds the thumbnail into each chapter file.\n")
	fmt.Fprintf(os.Stderr, "Accepts full YouTube URLs, video IDs, playlist URLs/IDs, or search terms.\n")
	fmt.Fprintf(os.Stderr, "Uses archive file '%s' located in the same directory as the executable to avoid reprocessing videos.\n\n", archiveFilename)
	fmt.Fprintf(os.Stderr, "Important:\n")
	fmt.Fprintf(os.Stderr, " - Requires 'yt-dlp' AND 'ffmpeg' to be installed and accessible in your system's PATH.\n")
	fmt.Fprintf(os.Stderr, " - Remember to quote arguments containing special shell characters (like '&', '?', '=')\n")
	fmt.Fprintf(os.Stderr, "   Example: %s 'https://www.youtube.com/watch?v=dQw4w9WgXcQ&list=...' 'Some Video ID'\n", appName)
}

func loadArchive(archivePath string) error {
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
	archiveMutex.Lock()
	defer archiveMutex.Unlock()
	if _, exists := processedArchive[identifier]; exists {
		return nil
	}
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

// processVideo handles fetching, downloading, splitting, and thumbnail embedding for one item.
func processVideo(videoIdentifier string, archivePath string, wg *sync.WaitGroup, itemNumber int) {
	defer itemsDone.Add(1)
	defer wg.Done()

	currentItemNum := itemsDone.Load() + 1
	log.Printf("--- Processing [%d/%d]: %s ---", currentItemNum, totalItems, videoIdentifier)

	// --- 1. Get Video Title ---
	log.Printf("INFO [%d/%d] %s: Fetching video title...", itemNumber, totalItems, videoIdentifier)
	videoTitle, err := getVideoTitle(videoIdentifier)
	if err != nil {
		log.Printf("ERROR [%d/%d] %s: Failed to get video title: %v", itemNumber, totalItems, videoIdentifier, err)
		errorCount.Add(1)
		return
	}
	sanitizedTitle := sanitizeFilename(videoTitle)
	outputDir := filepath.Join(outputBaseDir, sanitizedTitle)
	err = os.MkdirAll(outputDir, 0755)
	if err != nil {
		log.Printf("ERROR [%d/%d] %s: Failed to create output directory '%s': %v", itemNumber, totalItems, videoIdentifier, outputDir, err)
		errorCount.Add(1)
		return
	}
	log.Printf("INFO [%d/%d] %s: Title: '%s'. Output dir: '%s'", itemNumber, totalItems, videoIdentifier, videoTitle, outputDir)


	// --- 2. Run yt-dlp ---
	outputTemplateBase := filepath.Join(outputDir, "%(title)s")
	outputTemplateChapter := fmt.Sprintf("chapter:%s - %%(section_number)s - %%(section_title)s.%%(ext)s", outputTemplateBase)
	outputTemplateThumbnail := fmt.Sprintf("thumbnail:%s.%%(ext)s", outputTemplateBase)


	cmd := exec.Command("yt-dlp",
		"--color", "always",
		"--default-search", "ytsearch",
		"-i",
		"-f", "bestaudio/best",
		"--extract-audio",
		"--audio-format", "mp3",
		"--audio-quality", "0",
		"--split-chapters",
		"--write-thumbnail",
		"-o", outputTemplateChapter,
		"-o", outputTemplateThumbnail,
		videoIdentifier,
	)

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

	var streamWg sync.WaitGroup
	streamWg.Add(2)
	go func() {
		defer streamWg.Done()
		_, _ = io.Copy(os.Stdout, stdoutPipe)
	}()
	go func() {
		defer streamWg.Done()
		_, _ = io.Copy(os.Stderr, stderrPipe)
	}()
	streamWg.Wait()

	err = cmd.Wait()
	ytDlpFailed := (err != nil)

	// --- 3. Handle yt-dlp Result ---
	doneCount := itemsDone.Load() + 1

	if ytDlpFailed {
		log.Printf("--- ERROR Finished (yt-dlp) [%d/%d]: %s (yt-dlp error: %v) ---", doneCount, totalItems, videoIdentifier, err)
		errorCount.Add(1)
	} else {
		log.Printf("INFO [%d/%d] %s: yt-dlp finished successfully. Starting thumbnail embedding...", itemNumber, totalItems, videoIdentifier)

		// --- 4. Post-processing: Embed Thumbnails ---
		embedErrors, embeddingWasAttempted := embedThumbnailsInChapters(outputDir, videoTitle, videoIdentifier, itemNumber)

		// --- 5. Final Status and Archiving ---
		finalStatus := ""
		archiveErr := appendToArchive(archivePath, videoIdentifier)
		if archiveErr != nil {
			log.Printf("--- ERROR Finished [%d/%d]: %s (Failed to update archive: %v) ---", doneCount, totalItems, videoIdentifier, archiveErr)
			errorCount.Add(1)
		} else {
			processedCount.Add(1)
			if !embeddingWasAttempted {
				finalStatus = "Archived, No Thumbnail Found/Needed"
			} else if embedErrors > 0 {
				finalStatus = fmt.Sprintf("Archived, Thumbnails Embedded (%d Errors)", embedErrors)
			} else {
				finalStatus = "Archived, Thumbnails Embedded Successfully"
			}
			log.Printf("--- Success Finished [%d/%d]: %s (%s) ---", doneCount, totalItems, videoIdentifier, finalStatus)
		}
	}
}

// embedThumbnailsInChapters finds chapters and the thumbnail, then uses ffmpeg to embed.
func embedThumbnailsInChapters(outputDir string, videoTitle string, videoIdentifier string, itemNumber int) (embedErrorCount int, embeddingAttempted bool) {
	embedErrorCount = 0
	embeddingAttempted = false

	// --- Find the Thumbnail File ---
	var thumbnailPath string
	files, err := os.ReadDir(outputDir) // Use os.ReadDir which doesn't require io/fs import
	if err != nil {
		if os.IsNotExist(err) {
			log.Printf("ERROR [%d/%d] %s: Output directory '%s' disappeared? Cannot embed thumbnails.", itemNumber, totalItems, videoIdentifier, outputDir)
		} else {
			log.Printf("ERROR [%d/%d] %s: Failed to read output directory '%s': %v", itemNumber, totalItems, videoIdentifier, outputDir, err)
		}
		return 1, false
	}

	expectedThumbnailBaseName := videoTitle
	found := false
	for _, file := range files { // file is os.DirEntry here
		if !file.IsDir() {
			name := file.Name()
			ext := filepath.Ext(name)
			baseName := strings.TrimSuffix(name, ext)

			if baseName == expectedThumbnailBaseName {
				lowerExt := strings.ToLower(ext)
				for _, knownExt := range thumbnailExtensionsList {
					if lowerExt == knownExt {
						thumbnailPath = filepath.Join(outputDir, name)
						found = true
						break
					}
				}
			}
		}
		if found {
			break
		}
	}

	if !found {
		log.Printf("WARN [%d/%d] %s: No thumbnail file found in '%s' matching base name '%s' and known extensions %v. Skipping embedding.", itemNumber, totalItems, videoIdentifier, outputDir, expectedThumbnailBaseName, thumbnailExtensionsList)
		return 0, false
	}

	log.Printf("INFO [%d/%d] %s: Found thumbnail: %s", itemNumber, totalItems, videoIdentifier, filepath.Base(thumbnailPath))
	embeddingAttempted = true

	// --- Find Chapter MP3 Files ---
	chapterPattern := filepath.Join(outputDir, "*.mp3")
	chapterPaths, err := filepath.Glob(chapterPattern)
	if err != nil {
		log.Printf("ERROR [%d/%d] %s: Error searching for chapter MP3 files in '%s' using pattern '%s': %v", itemNumber, totalItems, videoIdentifier, outputDir, chapterPattern, err)
		embedErrorCount++
		return embedErrorCount, embeddingAttempted
	}
	if len(chapterPaths) == 0 {
		log.Printf("WARN [%d/%d] %s: No chapter MP3 files found in '%s' matching '%s'. Skipping embedding.", itemNumber, totalItems, videoIdentifier, outputDir, chapterPattern)
		cleanupErr := os.Remove(thumbnailPath)
		if cleanupErr != nil {
			log.Printf("WARN [%d/%d] %s: Could not remove unused thumbnail file '%s': %v", itemNumber, totalItems, videoIdentifier, thumbnailPath, cleanupErr)
		}
		return 0, embeddingAttempted
	}
	log.Printf("INFO [%d/%d] %s: Found %d chapter files matching pattern. Starting ffmpeg embedding...", itemNumber, totalItems, videoIdentifier, len(chapterPaths))

	// --- Loop and Embed using ffmpeg (Sequentially) ---
	// var embedWg sync.WaitGroup // Removed unused variable
	for _, chapterPath := range chapterPaths {
		if _, statErr := os.Stat(chapterPath); os.IsNotExist(statErr) || strings.HasSuffix(chapterPath, ".tmp_thumb.mp3") {
			continue
		}

		log.Printf("INFO [%d/%d] %s: Embedding thumbnail into: %s", itemNumber, totalItems, videoIdentifier, filepath.Base(chapterPath))
		tempOutputFile := chapterPath + ".tmp_thumb.mp3"
		_ = os.Remove(tempOutputFile)

		ffmpegCmd := exec.Command("ffmpeg",
			"-i", chapterPath,
			"-i", thumbnailPath,
			"-map", "0:a",
			"-map", "1:v",
			"-c:a", "copy",
			"-c:v", "copy",
			"-disposition:v", "attached_pic",
			"-id3v2_version", "3",
			"-metadata:s:v", "title=Album cover",
			"-metadata:s:v", "comment=Cover (front)",
			"-y",
			tempOutputFile,
		)

		var ffmpegStderr bytes.Buffer
		ffmpegCmd.Stderr = &ffmpegStderr

		err = ffmpegCmd.Run()
		if err != nil {
			log.Printf("ERROR [%d/%d] %s: ffmpeg failed for '%s': %v\nffmpeg stderr: %s",
				itemNumber, totalItems, videoIdentifier, filepath.Base(chapterPath), err, ffmpegStderr.String())
			embedErrorCount++
			_ = os.Remove(tempOutputFile)
			continue
		}

		// --- Replace Original with Temp File ---
		errRemove := os.Remove(chapterPath)
		if errRemove != nil && !os.IsNotExist(errRemove) {
			log.Printf("ERROR [%d/%d] %s: Failed to remove original chapter file '%s' before rename: %v", itemNumber, totalItems, videoIdentifier, filepath.Base(chapterPath), errRemove)
			embedErrorCount++
			_ = os.Remove(tempOutputFile)
			continue
		}

		errRename := os.Rename(tempOutputFile, chapterPath)
		if errRename != nil {
			log.Printf("ERROR [%d/%d] %s: Failed to rename temp file to '%s': %v", itemNumber, totalItems, videoIdentifier, filepath.Base(chapterPath), errRename)
			embedErrorCount++
			_ = os.Remove(tempOutputFile)
		}
	} // End chapter loop

	if embedErrorCount > 0 {
		log.Printf("WARN [%d/%d] %s: Finished embedding phase with %d errors for %d chapters.", itemNumber, totalItems, videoIdentifier, embedErrorCount, len(chapterPaths))
	} else {
		log.Printf("INFO [%d/%d] %s: Finished embedding phase successfully for all %d processed chapters.", itemNumber, totalItems, videoIdentifier, len(chapterPaths))
	}

	// --- Cleanup: Remove the original downloaded thumbnail file ---
	log.Printf("INFO [%d/%d] %s: Cleaning up thumbnail file: %s", itemNumber, totalItems, videoIdentifier, thumbnailPath)
	err = os.Remove(thumbnailPath)
	if err != nil {
		log.Printf("WARN [%d/%d] %s: Could not remove original thumbnail file '%s': %v", itemNumber, totalItems, videoIdentifier, thumbnailPath, err)
	}

	return embedErrorCount, embeddingAttempted
}
