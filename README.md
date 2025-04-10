# multidl-ytdlp

Initially started as an alternative to my bash script ytmp3.sh that uses yt-dlp
to download videos at best quality possible, transform them into mp3 files splitted using the video's chapters and adding the video miniature to the files.

Searching for a way to run multiple instances of yt-dlp at once I didn't find a straigtforward and kind of automatic solution so i made that script to do just that. 

This go script has a broader purpose of running ytdlp with some concurrency aspecs using Goroutines.
The current version is the same as the bash script but it would be easy to adapt to only download videos (and not transform those afterwards, etc...).
