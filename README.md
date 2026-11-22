A simple, scalable Telegram bot for downloading YouTube videos and converting them to audio.

The main goal of this project is to provide a convenient interface for downloading music from YouTube videos.

This bot accepts a video link and returns an MP3 audio file with pre-populated metadata and cover art.

-Download engine - yt-dlp
-Authorization is performed via a separate Firefox container (localhost 3000)
-Interface - Telegram
-Queue - Redis
-Workers - Celery

Launch and scaling are performed via Docker.
Before starting, you need to create a YouTube session in the Firefox container and create an .env file with the bot token.
