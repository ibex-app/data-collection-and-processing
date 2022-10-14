from ibex_models import ProcessTaskBatch, Post
from beanie.odm.operators.find.comparison import In
import codecs, csv
from uuid import UUID

class TopEngagementProcessor:

    async def process(self, task:ProcessTaskBatch):
        """Makes csvs of the top 100 engaged with posts per platform for this monitor"""
        prefix = '../csvs/'
        # all_posts: List[Post] = await Post.find().to_list()
        
        # posts=[]
        # for x in all_posts:
            # if In(Post.monitor_ids, task.monitor_id):
                # posts.append(x)
        result = {}
        posts:List[Post] = await Post.find(In(Post.monitor_ids, [task.monitor_id]), ).to_list()
        for post in posts:
            platform = post.platform
            try:
                result[platform]
            except KeyError:
                result[platform] ={}
            
            engagement = post.getEngagement()
            post = post.to_dict()
            post['engagement'] = engagement
            try:
                result[platform][engagement].append(post)
            except KeyError:
                result[platform][engagement] = [post]
            
        outs={}
        for platform, v in result.items():
            ct=0
            outs[platform] = []
            for engagement, posts in sorted(v.items(), reverse=True):
                for post in posts:
                    ct+=1
                    if ct > 100:
                        break
                    outs[platform].append(post)
        
        # for k,v in outs.items():
        #     if len(v) > 0:
        #         with codecs.open(prefix+'%s_%s.csv' % (k, task.monitor_id),'w', encoding='utf-8') as f:
        #             w=csv.DictWriter(f, fieldnames=list(v[0].keys()))
        #             w.writeheader()
        #             w.writerows(v)
