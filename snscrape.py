import praw
from pymongo import MongoClient, errors
from datetime import datetime
import re
import threading
import logging
import time

# 配置日志记录
logging.basicConfig(
    level=logging.DEBUG,  # 设置为DEBUG级别以捕获更多信息
    format='%(asctime)s %(levelname)s:%(message)s',
    handlers=[
        logging.FileHandler("scraper.log"),
        logging.StreamHandler()
    ]
)

# 配置
REDDIT_CLIENT_ID = '7FHlHgGZTLNuH2OicRbiQw'  # 替换为您的client_id
REDDIT_CLIENT_SECRET = 'M479-v5WNByCiGTm_NKfx8V9aCHVwg'  # 替换为您的client_secret
REDDIT_USER_AGENT = 'Message About by /u/Nervous-Pound-5482'  # 替换为您的user_agent

MONGO_URI = "mongodb://localhost:27017/"  # 如果使用远程MongoDB，请修改为相应的URI
DB_NAME = "reddit_monitor"
COLLECTION_NAME = "posts_comments"
KEYWORDS_COLLECTION = "keywords"  # 关键词集合名称

# 连接Reddit
try:
    reddit = praw.Reddit(client_id=REDDIT_CLIENT_ID,
                         client_secret=REDDIT_CLIENT_SECRET,
                         user_agent=REDDIT_USER_AGENT,
                         check_for_async=False)  # 确保同步模式
    # 通过尝试访问一个已知的 subreddit 来验证连接
    subreddit = reddit.subreddit('test')
    _ = subreddit.id  # 简单的访问调用
    logging.info("Reddit connection established.")
except Exception as e:
    logging.error(f"Failed to connect to Reddit: {e}")
    exit(1)

# 连接MongoDB
try:
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    collection = db[COLLECTION_NAME]
    keywords_collection = db[KEYWORDS_COLLECTION]
    # 创建唯一索引
    collection.create_index('id', unique=True)
    keywords_collection.create_index('keyword', unique=True)
    logging.info("Connected to MongoDB and ensured indexes.")
except Exception as e:
    logging.error(f"Failed to connect to MongoDB: {e}")
    exit(1)

def get_keywords():
    """从MongoDB中获取当前激活的关键词列表"""
    try:
        keywords = list(keywords_collection.find({"active": True}, {"_id": 0, "keyword": 1}))
        keyword_list = [item["keyword"] for item in keywords]
        logging.debug(f"Retrieved active keywords: {keyword_list}")
        return keyword_list
    except Exception as e:
        logging.error(f"Failed to retrieve keywords: {e}")
        return []

def compile_keyword_patterns(keywords):
    """编译关键词正则表达式模式列表，不使用词边界以支持更多场景"""
    patterns = [re.compile(re.escape(k), re.IGNORECASE) for k in keywords]
    logging.debug(f"Compiled regex patterns: {[p.pattern for p in patterns]}")
    return patterns

def contains_keyword(text, patterns):
    """检查文本是否包含任意一个关键词，并记录匹配的关键词"""
    matched_keywords = []
    for pattern in patterns:
        if pattern.search(text):
            matched_keywords.append(pattern.pattern)
    if matched_keywords:
        logging.debug(f"Matched keywords: {matched_keywords} in text: {text[:30]}...")
        return True
    return False

def process_submission(submission, patterns):
    try:
        title = submission.title or ""
        selftext = submission.selftext or ""
        if contains_keyword(title, patterns) or contains_keyword(selftext, patterns):
            doc = {
                'type': 'submission',
                'id': submission.id,
                'title': title,
                'selftext': selftext,
                'score': submission.score,
                'url': submission.url,
                'created_at': datetime.utcfromtimestamp(submission.created_utc),
                'author': str(submission.author),
                'subreddit': str(submission.subreddit),
                'body': '',
                'link': '',
                'completed': False
            }
            try:
                collection.insert_one(doc)
                logging.info(f"Inserted submission: {title[:30]}...")
            except errors.DuplicateKeyError:
                logging.debug(f"Duplicate submission skipped: {submission.id}")
    except Exception as e:
        logging.error(f"Error processing submission {submission.id}: {e}")

def process_comment(comment, patterns):
    try:
        body = comment.body or ""
        if contains_keyword(body, patterns):
            doc = {
                'type': 'comment',
                'id': comment.id,
                'body': body,
                'score': comment.score,
                'link': f"https://reddit.com{comment.permalink}",
                'created_at': datetime.utcfromtimestamp(comment.created_utc),
                'author': str(comment.author),
                'subreddit': str(comment.subreddit),
                'title': '',
                'selftext': '',
                'url': '',
                'completed': False
            }
            try:
                collection.insert_one(doc)
                logging.info(f"Inserted comment: {body[:30]}...")
            except errors.DuplicateKeyError:
                logging.debug(f"Duplicate comment skipped: {comment.id}")
    except Exception as e:
        logging.error(f"Error processing comment {comment.id}: {e}")

def stream_submissions(patterns):
    logging.info("Starting submission stream...")
    while True:
        try:
            for submission in reddit.subreddit('all').stream.submissions(skip_existing=True):
                process_submission(submission, patterns)
        except Exception as e:
            logging.error(f"Error in submission stream: {e}")
            time.sleep(60)  # 等待一分钟后重试

def stream_comments(patterns):
    logging.info("Starting comment stream...")
    while True:
        try:
            for comment in reddit.subreddit('all').stream.comments(skip_existing=True):
                process_comment(comment, patterns)
        except Exception as e:
            logging.error(f"Error in comment stream: {e}")
            time.sleep(60)  # 等待一分钟后重试

def main():
    patterns = []
    while True:
        keywords = get_keywords()
        if not keywords:
            logging.warning("No active keywords found in the 'keywords' collection.")
            time.sleep(60)  # 暂停一分钟后重试
            continue

        new_patterns = compile_keyword_patterns(keywords)
        if new_patterns != patterns:
            patterns = new_patterns
            logging.info(f"Updated monitoring keywords: {keywords}")
            # 启动新的监听线程
            t1 = threading.Thread(target=stream_submissions, args=(patterns,), daemon=True)
            t2 = threading.Thread(target=stream_comments, args=(patterns,), daemon=True)
            t1.start()
            t2.start()

        time.sleep(300)  # 每5分钟检查一次关键词更新

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logging.info("Stopping scraper...")
