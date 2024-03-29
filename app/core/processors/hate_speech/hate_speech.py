from ibex_models import ProcessTask, Post
from app.core.datasources.utils import get_query_with_declancions

class HateSpeechProcessor:
    async def process(self, task:ProcessTask):
        #TODO: configurable lists of hate speech plus per language. Hardcoded Russian stuff for now
        hatespeech = ['социально чуждый элемент', 'антисоветский элемент','кулак','враг народа']
        
        post = task.post
        text = f'{task.post.title} {task.post.text}'
        if task.post.transcripts and len(task.post.transcripts):
                text += ' '.join([transcript.text for transcript in task.post.transcripts])
        post.hatespeech_terms = []
        if text and text.strip():
            for search_term in hatespeech:
                eldar_query = get_query_with_declancions(search_term)
                if len(eldar_query.filter([text])) > 0:
                    if search_term not in post.hatespeech_terms: post.hatespeech_terms.append(search_term)
        post.save()

    async def apply_topic(post, topic, terms):
        text = post.api_dump
        if post.transcripts and len(post.transcripts):
            text += ' '.join([transcript.text for transcript in post.transcripts])
        
        for term in terms:
            eldar_query = get_query_with_declancions(term)

            if len(eldar_query.filter([text])) > 0:
                if term not in post.labels[topic]:
                    post.labels[topic].append(term)
        await post.save()

    def get_terms():
        return {
            'topic1': ['социально чуждый элемент', 'антисоветский элемент','кулак','враг народа'],
            'topic2': ['социально чуждый элемент', 'антисоветский элемент','кулак','враг народа'],
            'topic3': ['социально чуждый элемент', 'антисоветский элемент','кулак','враг народа'],
            'topic4': ['социально чуждый элемент', 'антисоветский элемент','кулак','враг народа'],
        }