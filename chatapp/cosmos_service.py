from azure.cosmos import CosmosClient, exceptions
import os


class CosmosService:

    def __init__(self) -> None:
        """CosmosDBのクライアントを初期化する。
        """
        self.client = CosmosClient(
            url=os.getenv('COSMOS_URL'),
            credential=os.getenv('COSMOS_CREDENTIAL')
        )
        self.database = self.client.get_database_client(
            os.getenv('COSMOS_DATABASE_NAME'))
        self.container = self.database.get_container_client(
            os.getenv('COSMOS_CONTAINER_NAME'))

    def get_items_by_vector(self, embedding, VECTOR_SCORE_THRESHOLD) -> list:
        """ベクトル検索でCosmosDBからアイテムを取得する。

        Args:
            embedding (_type_): 検索クエリのベクトル値
            VECTOR_SCORE_THRESHOLD (_type_): ベクトルスコアのしきい値

        Returns:
            list: CosmosDBから取得したアイテムのリスト
        """
        try:
            print('🚀Querying CosmosDB.')
            print(f'🚀vectorScore: {VECTOR_SCORE_THRESHOLD}')

            query = f"""SELECT TOP 10 c.file_name, c.page_number, c.content, 
            VectorDistance(c.vector, @embedding) AS SimilarityScore 
            FROM c 
            WHERE VectorDistance(c.vector, @embedding) > {VECTOR_SCORE_THRESHOLD} 
            ORDER BY VectorDistance(c.vector, @embedding)"""
            parameters = [
                {'name': '@embedding', 'value': embedding}
            ]

            items = self.container.query_items(
                query=query,
                parameters=parameters,
                enable_cross_partition_query=True
            )

            resources = [item for item in items]
            for item in resources:
                print(
                    f'🚀{item["file_name"]}, {item["page_number"]}, {item["content"]}, {item["SimilarityScore"]} is a capitol \n')

            return resources

        except exceptions.CosmosHttpResponseError as e:
            print(f'❌CosmosHttpResponseError at get_items_by_vector: {e}')
            raise e

        except Exception as e:
            print(f'❌Error at get_items_by_vector: {e}')
            raise e
