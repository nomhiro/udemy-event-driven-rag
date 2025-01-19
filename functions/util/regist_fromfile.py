import uuid

from domain.cosmos_document import CosmosDocument

from util.cosmos_service import CosmosService
from util.gen_keywords import extract_keywords_from_file_path


def regist_text(cosmos_service: CosmosService,
                file_name: str,
                file_path: str,
                content: str,
                BLOB_NAME: str):

    # ファイル名をタイトルとして、コンテンツをMarkdown形式に変換
    content = '# ' + file_name + '\n\n' + content,

    keywords = extract_keywords_from_file_path(file_path, BLOB_NAME)

    # CosmosDBに登録するアイテムのオブジェクト
    cosmos_page_obj = CosmosDocument(
        id=str(uuid.uuid4()),
        file_name=file_name,
        file_path=file_path,
        page_number=0,
        content=content[0],
        vector=[],
        keywords=keywords,
        delete_flag=False,
        vector_update_flag=True
    )

    cosmos_service.upsert_item(cosmos_page_obj.to_dict())
