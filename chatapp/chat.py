import streamlit as st
from streamlit_chat import message
import os

from openai_service import AzureOpenAIService
from cosmos_service import CosmosService

# ENVIRONMENT VARIABLES
AOAI_CHAT_DEPLOYMENT = os.getenv("AOAI_CHAT_DEPLOYMENT")
VECTOR_SCORE_THRESHOLD = float(os.getenv("VECTOR_SCORE_THRESHOLD"))

# PROMPT SETUP
system_prompt_chat = """あなたはAIアシスタントです。問い合わせに対し「# 検索結果」の内容をもとに回答してください。

# 制約条件
- 検索結果がない場合は、一般的な情報から回答できる場合は回答してください。回答できない場合は不明瞭な回答をしてはいけません。
- 構造的に回答する必要がある場合は、Markdownで構造的に回答してください。
- チャット履歴の「参考情報」は無視してください。
- 「参考情報」はシステムが自動で付与しています。あなたの回答に含めてはいけません。
"""

# Client
aoai_service = AzureOpenAIService()
cosmos_service = CosmosService()

# seed message
seed_message = {"role": "system", "content": system_prompt_chat}

# SESSION MANAGEMENT
if "chat_messages" not in st.session_state:
    st.session_state["chat_messages"] = []

# PAGE SETUP
st.set_page_config(layout="wide")
st.title("🚀Udemy RAG")

# SIDEBAR SETUP
clear_button = st.sidebar.button("Clear Conversation", key="clear")
if clear_button:
    st.session_state["chat_messages"] = []

# with container:
user_message = st.chat_input("user:")
assistant_text = ""

# 過去のチャットメッセージを表示
for text_info in st.session_state["chat_messages"]:
    with st.chat_message(text_info["role"], avatar=text_info["role"]):
        st.write(text_info["content"])

# ユーザーの入力がある場合
if user_message:
    # ユーザーの入力を表示
    with st.chat_message("user", avatar="user"):
        st.write(user_message)

    # ユーザーの入力をchat_messagesに追加
    st.session_state["chat_messages"].append(
        {"role": "user", "content": user_message})

    # CosmosDBでベクトル検索
    search_items = cosmos_service.get_items_by_vector(
        aoai_service.getEmbedding(
            input=user_message
        ),
        VECTOR_SCORE_THRESHOLD
    )

    # システムメッセージに検索結果を追加
    system_message = system_prompt_chat + "\n\n# 検索結果"
    # 画面に表示する検索結果
    display_searched_file_name = "\n\n---\n #### 参考情報"
    for index, result in enumerate(search_items):
        # ループ番号を付与してファイルの内容をシステムメッセージに追加
        system_message += f'\n\n--- {index + 1} ---\n{result["content"]}'
        # 画面に表示する検索結果を追加
        display_searched_file_name += f'\n{index + 1}. {result["file_name"]}  (page{result["page_number"]})  : {result["SimilarityScore"]}'
    print(f"system_message: {system_message}")

    # OpenAIリクエスト用のメッセージ
    messages = [
        {"role": "system", "content": system_message},
        *st.session_state["chat_messages"],
    ]

    # OpenAI Chat APIで回答を取得
    response = aoai_service.openai.chat.completions.create(
        model=AOAI_CHAT_DEPLOYMENT,
        messages=[
            {"role": "system", "content": system_message},
            *st.session_state["chat_messages"],
        ],
        stream=True,
    )
    # AIからの回答をStreamで表示
    with st.chat_message("assistant", avatar="assistant"):
        place_assistant = st.empty()
        for chunk in response:
            if chunk.choices:
                content = chunk.choices[0].delta.content
                if content:
                    assistant_text += content
                    place_assistant.write(assistant_text)
            else:
                content = None

        # 検索結果があれば、検索結果をAI回答の末尾に追加
        place_append = st.empty()
        if search_items:
            place_append.write(display_searched_file_name)

    # AIの回答をchat_messagesに追加
    st.session_state["chat_messages"].append(
        {"role": "assistant", "content": assistant_text}
    )
