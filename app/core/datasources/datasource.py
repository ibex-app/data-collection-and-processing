from typing import List, Dict
from ibex_models import Post, CollectTask
from abc import ABC, abstractmethod


class Datasource(ABC):
    """The abstract class for data collectors.
        
        All data collectors/data sources implement 
        methods described below.
    """

    @abstractmethod
    async def collect(self, collect_task: CollectTask) -> List[Post]:
        """The method is responsible for collecting posts
            from platforms.

        Args:
            collect_action(CollectTask): CollectTask object holds 
                all the metadata needed for data collection.

        Returns:
            (List[Post]): List of collected posts.
        """
        pass

    @abstractmethod
    async def get_hits_count(self, collect_task: CollectTask) -> int:
        """The method is responsible for collecting the number of posts, 
            that satisfy all criterias in CollectTask object.
            
        Note:
            Do not collect actual posts here, this method is only 
            applicable to platforms that provide this kind of information.

        Args:
            collect_action(CollectTask): CollectTask object holds 
                all the metadata needed for data collection.

        Returns:
            (int): Number of posts existing on the platform.
        """
        pass
    
    # @staticmethod
    @abstractmethod
    def map_to_post(api_post: Dict, collect_task: CollectTask) -> Post:
        """The method is responsible for mapping data redudned by plarform api
            into Post class.
            
        Args:
            api_post: responce from platform API.
            collect_action(CollectTask): the metadata used for data collection task.

        Returns:
            (Post): class derived from API data.
        """
        pass

    @abstractmethod
    def map_to_posts(self, posts: List[Dict], collect_task: CollectTask) -> List[Post]:
        pass
