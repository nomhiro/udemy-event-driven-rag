import logging
import os
import openai
from pydantic import BaseModel

# ENVIRONMENT VARIABLES
AOAI_CHAT_DEPLOYMENT = os.getenv("AOAI_CHAT_DEPLOYMENT")


class AzureOpenAIService:

    def __init__(self) -> None:
        self.openai = openai
        self.openai.api_type = "azure"
        self.openai.azure_endpoint = os.getenv("AOAI_ENDPOINT")
        self.openai.api_version = os.getenv("AOAI_API_VERSION")
        self.openai.api_key = os.getenv("AOAI_API_KEY")

    def getEmbedding(self, input) -> list:
        """ベクトル値を取得する。

        Args:
            input (_type_): ベクトル化する文字列

        Returns:
            list: ベクトル値
        """
        try:
            response = self.openai.embeddings.create(
                input=input,
                model=os.getenv("AOAI_EMBEDDING_DEPLOYMENT")
            )
            return response.data[0].embedding
        except Exception as e:
            logging.error(f'❌Error at getEmbedding: {e}')
            raise e
