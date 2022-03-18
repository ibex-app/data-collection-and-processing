from beanie import init_beanie
import motor
from ibex_models import Post, MediaStatus
import asyncio

async def save(): 
    file_ = open('nohup_worker_3.out', 'r')
    lines = file_.readlines()
    
    transcripts = {}
    post_id = None
    for line_index, line in enumerate(lines):
        if 'in process task' in line:
            if post_id:
                transcripts[post_id] = post_transcripts
            post_id = line.split(' ')[4]
            post_transcripts = []
        if post_id and " {'text': '" in line:
            transcript_text = line.split(" {'text': '")[1].replace("'}", '')
            if transcript_text.strip() != '':
                second = lines[line_index - 1].split('/')[5].split('.')[0]
                post_transcripts.append({ 'second': second, 'text': transcript_text })
            
    transcripts[post_id] = post_transcripts
    
    mongodb_connection_string = "mongodb+srv://root:Dn9B6czCKU6qFCj@cluster0.iejvr.mongodb.net/ibex?retryWrites=true&w=majority"
    client = motor.motor_asyncio.AsyncIOMotorClient(mongodb_connection_string)
    await init_beanie(database=client['ibex'], document_models=[Post])

    for psot_id, transcript in transcripts.items():
        post = await Post.get(psot_id)
        post.transcripts = transcript

        post.media_status = MediaStatus.processed
        await post.save()

asyncio.run(save())