import yt_dlp

hashtag = input("Insert what type of videos you'd like to download: ")
searches = int(input("Enter how many searches you want: "))

def download(hash, results):

    query = hash if hash.startswith("#") else f"#{hash}"

    ydl_opts = {
        
        'default_search': f'ytsearch{results}',
        'format': 'bestvideo+bestaudio/best',
        'outtmpl': '%(title)s.%(ext)s',
        'quiet': False,
        'javascript_runtimes': ['node'],        
        'cookiesfrombrowser': 'firefox,',
        'extractor_args': {'youtube': {'js_runtimes' ['node']}},
    }


    with yt_dlp.YoutubeDL(ydl_opts) as ydl:

        try:
            ydl.download([query])
            print(f"\n Downloads completed")
        except Exception as e:
            print(f"\n Error happend when downloading: {e}")



download(hashtag, searches)
