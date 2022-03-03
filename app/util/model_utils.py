from enum import Enum
from typing import Dict
import pickle
import base64

from bson import json_util
import json

def serialize_to_base64(obj: object):
    """
    :param obj: object
    :return: base64 encoding of an object.
    """
    obj = pickle.dumps(obj)
    obj = base64.b64encode(obj)
    obj = obj.decode("ascii")
    
    return json_util.dumps(obj) + '_[SEP]_' + obj


def deserialize_from_base64(obj: str):
    """
    :param obj: base64 encoded string.
    :return: object
    """
    
    obj = base64.b64decode(obj.split('_[SEP]_')[1])
    obj = pickle.loads(obj)
    return obj


# def _get_class_members(clazz) -> Dict:
#     return vars(clazz)['__fields__']
#
#
# def _obj_to_typed_dict_nested_helper(val, clazz):
#     if issubclass(clazz, Enum) or clazz not in model_classes:
#         return val
#
#     val = val.__dict__
#     members = _get_class_members(clazz)
#     for name, member in members.items():
#         if member.name not in val or val[member.name] is None:
#             continue
#         if type(val[member.name]) is list:
#             arr = []
#             for item in val[member.name]:
#                 arr.append(_obj_to_typed_dict_nested_helper(item, member.type_))
#             val[member.name] = arr
#         else:
#             val[member.name] = _obj_to_typed_dict_nested_helper(val[member.name], member.type_)
#
#     return val
#
#
# def obj_to_typed_dict_nested(instance) -> Dict:
#     """
#     Turn a nested object into a dictionary.
#     :param instance:
#     :return: val:
#     """
#     clazz = type(instance)
#     val = _obj_to_typed_dict_nested_helper(instance, clazz)
#     return val


# async def test():
#     client = AsyncIOMotorClient('mongodb://127.0.0.1:27017/')
#     await init_beanie(database=client.ibex, document_models=[CollectAction, SearchTerm, DataSource, Post])
#
#     data_source = DataSource(
#         title="title",
#         platform=Platform.twitter,
#         platform_id="pid",
#         url="url",
#         program_title='ptitle',
#         tags=['***']
#     )
#     data_source2 = DataSource(
#         title="title",
#         platform=Platform.twitter,
#         platform_id="pid",
#         url="url",
#         program_title='ptitle2',
#         tags=['***']
#     )
#     data_source3 = DataSource(
#         title="title",
#         platform=Platform.twitter,
#         platform_id="pid",
#         url="url",
#         program_title='ptitle3',
#         tags=['***']
#     )
#     search_term = SearchTerm(
#         tags=["*"],
#         term="search term"
#     )
#     collect_task = CollectTask(
#         platform=Platform.twitter,
#         use_batch=True,
#         curated=True,
#         date_from=(datetime.now() - timedelta(hours=18)),
#         date_to=datetime.now(),
#         data_source=data_source,
#         data_sources=[data_source2, data_source3],
#         search_terms=[search_term]
#     )
#     task_dict = obj_to_typed_dict_nested(collect_task)
#     print(task_dict)
#
#
# if __name__ == "__main__":
#     asyncio.run(test())
