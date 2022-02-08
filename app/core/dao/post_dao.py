from typing import List

from app.model.post import Post


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
    return [e for e in collected_items if e.platform+e.platform_id not in posts_in_db]
