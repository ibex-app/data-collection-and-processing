from typing import List

from ibex_models import Post, CollectTask


async def remove_duplicates_from_db(collected_items: List[Post]) -> List[Post]:
    """
    An optimized method to
    remove duplicates in a Post list with respect to items in MongoDB,
    according to (platform, platform_id) attributes.

    Steps:
        1. Get all Post projections from db that match the given collected_items fields.
        2. Filter collected_items by removing all elements present in list obtained above.

    :param collected_items:
    :return: filtered_items:
    """
    platforms_and_platform_ids = set()
    for citem in collected_items:
        platforms_and_platform_ids.add(citem.platform+citem.platform_id)

    posts_in_db = await Post.find() \
        .aggregate([
            {
                "$project": {
                    "platform_and_platform_id": {
                        "$concat": ["$platform", "$platform_id"]
                    }
                }
            },
            {
                "$match": {
                    "platform_and_platform_id": {
                        "$in": list(platforms_and_platform_ids)
                    }
                }
            }]) \
        .to_list()
    posts_in_db = set([e['platform_and_platform_id'] for e in posts_in_db])
    new_posts = [e for e in collected_items if e.platform+e.platform_id not in posts_in_db]

    return new_posts


async def insert_posts(collected_posts: List[Post], collect_task: CollectTask):
    count_inserts = 0
    count_updates = 0
    count_existed = 0

    #Deduplication step can be performed after posts are inserted, 
    if len(collected_posts):
        for post in collected_posts:
            post_in_db = await Post.find_one(Post.platform_id == post.platform_id, Post.platform == post.platform)
            #check if post exists in database
            if not post_in_db:
                count_inserts += 1
                await post.save()
            elif collect_task.monitor_id not in post_in_db.monitor_ids:
                #add monitor id to existsing post
                count_updates += 1
                post_in_db.monitor_ids.append(collect_task.monitor_id)
                await post_in_db.save()
            else:
                count_existed += 1

    return count_inserts, count_updates, count_existed