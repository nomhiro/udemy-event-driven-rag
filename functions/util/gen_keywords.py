from urllib.parse import urlparse

# 引数で受け取ったファイルパスからファイル名と拡張子を取得するメソッド


def extract_keywords_from_file_path(file_path: str, blob_name: str) -> list:
    """
    BlobファイルのURLであるfile_pathからフォルダ名とファイル名を取得しキーワードリストとする。
    URL部分とファイル拡張子を削除し、BLOB_NAMEをキーワードから削除する。

    Args:
        file_path (str): ファイルパス
        blob_name (str): BLOB_NAME

    Returns:
        list: キーワードのリスト
    """
    parsed_url = urlparse(file_path)
    file_path = parsed_url.path.lstrip('/')
    keywords = file_path.split("/")
    keywords[-1] = keywords[-1].rsplit('.', 1)[0]  # ファイル拡張子を削除
    keywords = [keyword for keyword in keywords if keyword != blob_name]
    return keywords
