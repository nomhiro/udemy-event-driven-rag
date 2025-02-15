# udemy-event-driven-rag
Udemy講座「イベントドリブンなRAGアーキテクチャ」のソースコードです。

## チャットアプリ

Streamlitを使ったチャットアプリです。

前提
- VSCodeに「Azure App Service」の拡張機能がインストールされていること

### 1. 依存関係のインストール
Pythonのモジュールをインストール
```bash
pip install -r requirements.txt
```

### 2. アプリの起動
F5キーか、VSCode の Runボタンを押すとアプリが起動します。

### 3. App Service の設定
Azure Portal の App Service の「構成」設定にて、スタートアップコマンドを設定します。
```bash
python -m streamlit run chat.py --server.port 8000 --server.address 0.0.0.0
```

## CosmosDB Change Feed トリガー Functions
Azure Functions の CosmosDB Change Feed トリガーを使った関数です。

### 1. 依存関係のインストール
Pythonのモジュールをインストール
```bash
pip install -r requirements.txt
```

