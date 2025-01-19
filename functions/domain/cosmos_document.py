from pydantic import BaseModel
import uuid


class CosmosDocument(BaseModel):
    id: str
    page_number: int
    content: str
    vector: list
    keywords: list
    file_name: str
    file_path: str
    delete_flag: bool
    vector_update_flag: bool

    def __init__(self, **data):
        super().__init__(**data)
        # idがない場合はuuidを生成
        if 'id' not in data:
            self.id = str(uuid.uuid4())

    def to_dict(self):
        return {
            'id': self.id,
            'page_number': self.page_number,
            'content': self.content,
            'vector': self.vector,
            'keywords': self.keywords,
            'file_name': self.file_name,
            'file_path': self.file_path,
            'delete_flag': self.delete_flag,
            'vector_update_flag': self.vector_update_flag
        }

    @staticmethod
    def from_dict(data: dict):
        return CosmosDocument(
            id=data['id'],
            page_number=data['page_number'],
            content=data['content'],
            vector=data['vector'],
            keywords=data['keywords'],
            file_name=data['file_name'],
            file_path=data['file_path'],
            delete_flag=data['delete_flag'],
            vector_update_flag=data['vector_update_flag']
        )

    def __str__(self):
        return f'CosmosDocument(id={self.id}, page_number={self.page_number}, content={self.content}, vector={self.content_vector}, keywords={self.keywords}, file_name={self.file_name}, file_path={self.file_path}, delete_flag={self.delete_flag}, vector_update_flag={self.vector_update_flag})'
