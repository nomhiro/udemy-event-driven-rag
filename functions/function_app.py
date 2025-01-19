import azure.functions as func
import logging
import os
import json
from io import BytesIO

from azure.storage.blob import BlobServiceClient
from util.openai_service import AzureOpenAIService
from util.cosmos_service import CosmosService
from domain.cosmos_document import CosmosDocument
from util.regist_fromfile import regist_text

logging.basicConfig(level=logging.INFO)
app = func.FunctionApp()


# ENVIRONMENT VARIABLES
COSMOS_CONNECTION = os.getenv('COSMOS_CONNECTION')
COSMOS_DATABASE_NAME = os.getenv('COSMOS_DATABASE_NAME')
COSMOS_CONTAINER_NAME = os.getenv('COSMOS_CONTAINER_NAME')
TRIGGER_BLOB_CONTAINER = "rag-docs"
# IMAGE_BLOB_CONTAINER = "rag-images"
BLOB_CONNECTION = os.getenv("BLOB_CONNECTION")

# client
openai_service = AzureOpenAIService()
cosmos_service = CosmosService()
blob_service_client = BlobServiceClient.from_connection_string(
    BLOB_CONNECTION
)

# Azure OpenAI Service
STR_AI_SYSTEMMESSAGE = """
画像内の文字列をOCRし漏れなく出力しなさい。

##回答形式##
{
    "content":"画像の情報を漏れなく記載した文章。",
    "keywords": "カンマ区切りのキーワード群",
    "is_contain_image": "図や表などの画像で保存しておくべき情報が含まれている場合はtrue、それ以外はfalse"
}

##記載情報##
- content: 画像内の表は、Markdown形式で記載しなさい。
- keywords:  画像内の情報で重要なキーワードをkeywordsに記載してください。カンマ区切りで複数記載可能です。
- is_contain_image: 図や表などの画像で保存しておくべき情報が含まれている場合はtrue、それ以外はfalseを記載してください。
"""


@app.cosmos_db_trigger(arg_name="azcosmosdb", container_name=COSMOS_CONTAINER_NAME,
                       database_name=COSMOS_DATABASE_NAME, connection="COSMOS_CONNECTION")
def cosmosdb_trigger(azcosmosdb: func.DocumentList):
    logging.info('🚀 Python CosmosDB triggered.')

    try:
        for doc in azcosmosdb:
            doc_document = CosmosDocument(**doc)

            # ベクトル値更新フラグがTrueの場合、ベクトル値を更新する
            if doc_document.vector_update_flag:
                logging.info(
                    f'🚀 Start update vector : {doc_document.file_name}')
                # get vector from openai
                embedding = openai_service.getEmbedding(doc_document.content)

                # upsert item to cosmos
                doc_document.vector = embedding
                doc_document.vector_update_flag = False
                cosmos_service.upsert_item(doc_document.dict())
                logging.info(
                    f'🚀 End update vector : {doc_document.file_name}')

            else:
                logging.info(
                    f'🚀 Skip update vector : {doc_document.file_name}')

    except Exception as e:
        logging.error(f'❌Error at cosmosdb_trigger: {e}')
        raise e


@app.event_grid_trigger(arg_name="azeventgrid")
def EventGridTrigger(azeventgrid: func.EventGridEvent):
    event = json.dumps({
        'id': azeventgrid.id,
        'data': azeventgrid.get_json(),
        'topic': azeventgrid.topic,
        'subject': azeventgrid.subject,
        'event_type': azeventgrid.event_type,
    })
    event_dict = json.loads(event)  # eventを辞書型に変換
    blob_url = event_dict.get("data").get("url")

    logging.info('🚀 Python EventGrid trigger processed an event')
    logging.info(f"🚀 azeventgrid.get_json() : {azeventgrid.get_json()}")

    try:
        # event_typeがMicrosoft.Storage.BlobCreatedの場合
        if event_dict.get("event_type") == "Microsoft.Storage.BlobCreated":

            logging.info(f"🚀Event Type: {event_dict.get('event_type')}")

            # Blobのファイルパスを取得
            blob_file_path = blob_url.split(f"{TRIGGER_BLOB_CONTAINER}/")[1]
            logging.info(f"🚀Blob File Path: {blob_file_path}")
            blob_client = blob_service_client.get_blob_client(
                container=TRIGGER_BLOB_CONTAINER, blob=blob_file_path)
            blob_data = blob_client.download_blob()
            logging.info(f"🚀Blob File downloaded.")

            # Blobからダウンロードしたファイルのファイル名と拡張子を取得
            file_name = blob_data.name
            file_extension = os.path.splitext(file_name)[1]

            logging.info(f"🚀Blob Data: {blob_data}")
            logging.info(f"🚀Blob Name: {file_name}")
            logging.info(f"🚀Blob Extension: {file_extension}")

            ragdocs = blob_data.content_as_bytes()
            data_as_file = BytesIO(ragdocs)

            # 同じfile_nameがCosmosDBに存在する場合は、そのアイテムを削除する
            query = f"SELECT * FROM c WHERE c.file_path = \"{blob_url}\""
            items = cosmos_service.get_item(query)
            for item in items:
                cosmos_service.delete_item(item["id"])
                logging.info(
                    f"🚀Deleted data from CosmosDB: {item['file_name']}, {item['page_number']}")

            if file_extension == ".txt" or file_extension == ".md":

                logging.info("🚀Triggerd blob file is Text or Markdown")

                regist_text(
                    cosmos_service=cosmos_service,
                    file_name=file_name,
                    file_path=blob_url,
                    content=data_as_file.read().decode('utf-8'),
                    BLOB_NAME=TRIGGER_BLOB_CONTAINER
                )

            else:
                # 対応していない拡張子なので、ログにWarningで出力
                logging.warning(
                    f"🚀❌Unsupported File Extension: \"{file_extension}\", File Name: \"{data_as_file.name}\"")

        elif event_dict.get("event_type") == "Microsoft.Storage.BlobDeleted":
            # Blobが削除された場合の処理
            logging.info(f"🚀Event Type: {event_dict.get('event_type')}")

            # blob_urlをCosmosのfile_pathで検索し、CosmosDBのアイテムを取得
            query = f"SELECT * FROM c WHERE c.file_path = \"{blob_url}\""
            items = cosmos_service.get_item(query)

            for item in items:
                # CosmosDBのアイテムを削除
                cosmos_service.delete_item(item["id"])
                logging.info(f"🚀Deleted data from CosmosDB: {item}")

        else:
            # その他のイベントの場合
            logging.info(f"🚀Event Type: {event_dict.get('event_type')}")

    except Exception as e:
        logging.error(f"🚀❌Error at BlobTriggerEventGrid: {e}")
        raise e
