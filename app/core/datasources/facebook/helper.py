def needs_download(x): return "media_needs_to_be_downloaded" if (
    x == 'native_video' or x == 'live_video_complete') else \
    'ready_for_extraction'


def split_to_chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]
