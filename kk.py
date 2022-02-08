from beanie import init_beanie
from motor.motor_asyncio import AsyncIOMotorClient
from app.model.post_class import PostClass
import asyncio



connection_string = "mongodb+srv://root:Dn9nB6czCKU6qFCj@cluster0.iejvr.mongodb.net/ibex?retryWrites=true&w=majority"    
   

async def init_mongo():
    """
    Initialize a connection to MongoDB
    """
    client = AsyncIOMotorClient(connection_string)
    await init_beanie(database=client.ibex, document_models=[PostClass])


if __name__ == "__main__":
    asyncio.run(init_mongo())
