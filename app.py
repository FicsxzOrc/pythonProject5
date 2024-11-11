# app.py

import streamlit as st
from streamlit_cookies_manager import EncryptedCookieManager
from pymongo import MongoClient, errors
from datetime import datetime, timezone, timedelta
import pandas as pd
import re
import os
from dotenv import load_dotenv
import uuid

# -------------- 设置页面配置为宽布局 --------------
st.set_page_config(
    page_title="Reddit 实时关键词监控仪表板",
    layout="wide",
    initial_sidebar_state="expanded"
)

# -------------- 加载环境变量 --------------
load_dotenv()

# -------------- 初始化Cookies管理器 --------------
cookies = EncryptedCookieManager(
    password=os.getenv('COOKIE_PASSWORD', 'your_secure_random_password_here'),
    prefix='reddit_monitor_',
)

# -------------- 加载和保存cookies --------------
if not cookies.ready():
    # 等待cookie管理器准备就绪
    st.stop()

# -------------- 检查或创建用户ID --------------
if 'user_id' not in cookies:
    user_id = str(uuid.uuid4())
    cookies['user_id'] = user_id
    # 使用 session_state 触发渲染
    if 'rerun' not in st.session_state:
        st.session_state['rerun'] = True
else:
    user_id = cookies['user_id']

# -------------- 连接到 MongoDB --------------
@st.cache_resource
def get_db():
    try:
        client = MongoClient(os.getenv('MONGO_URI', 'mongodb://localhost:27017/'))
        db = client[os.getenv('DB_NAME', 'reddit_monitor')]
        collection = db[os.getenv('COLLECTION_NAME', 'posts_comments')]
        keywords_collection = db[os.getenv('KEYWORDS_COLLECTION', 'keywords')]
        preferences_collection = db[os.getenv('PREFERENCES_COLLECTION', 'preferences')]
        return collection, keywords_collection, preferences_collection
    except Exception as e:
        st.error(f"连接 MongoDB 失败: {e}")
        st.stop()

collection, keywords_collection, preferences_collection = get_db()

# -------------- 自动刷新（每10秒）--------------
try:
    from streamlit_autorefresh import st_autorefresh
    st_autorefresh(interval=10 * 1000, limit=None, key="autorefresh")
except ImportError:
    st.warning("请安装 `streamlit-autorefresh` 库以启用自动刷新功能。")
    st.info("通过 `pip install streamlit-autorefresh` 来安装。")

# -------------- 自定义 CSS --------------
st.markdown(
    """
    <style>
    .title {
        font-size: 40px;
        color: #1f77b4;
        font-weight: bold;
        text-align: center;
    }
    .stApp {
        padding-top: 1rem;
        padding-bottom: 1rem;
    }
    </style>
    """,
    unsafe_allow_html=True
)

# -------------- 标题 --------------
st.markdown('<p class="title">Reddit 实时关键词监控仪表板</p>', unsafe_allow_html=True)

# -------------- 初始化会话状态 --------------
if 'page_num' not in st.session_state:
    st.session_state.page_num = 1
if 'total_pages' not in st.session_state:
    st.session_state.total_pages = 1

# -------------- 定义分页函数 --------------
def previous_page():
    if st.session_state.page_num > 1:
        st.session_state.page_num -= 1

def next_page():
    if st.session_state.page_num < st.session_state.total_pages:
        st.session_state.page_num += 1

# 如果需要触发重新渲染（用户ID刚生成）
if st.session_state.get('rerun', False):
    st.session_state['rerun'] = False
    # 通过修改一个不存在的键来触发重新渲染
    st.session_state['dummy'] = True

# -------------- 侧边栏：过滤选项和关键词选择 --------------
st.sidebar.header("过滤选项")

# 日期范围过滤
st.sidebar.subheader("日期范围过滤")
today = datetime.now(timezone.utc).date()
start_date = st.sidebar.date_input("开始日期", today - timedelta(days=7), key='start_date_sidebar')
end_date = st.sidebar.date_input("结束日期", today, key='end_date_sidebar')

# 子版块过滤
st.sidebar.subheader("子版块过滤")
try:
    subreddits = collection.distinct("subreddit")
    selected_subreddits = st.sidebar.multiselect("选择子版块", ["全部"] + sorted(subreddits), default=["全部"], key='subreddits_sidebar')
except Exception as e:
    st.sidebar.error(f"获取子版块列表失败: {e}")
    selected_subreddits = ["全部"]

# 关键词选择
st.sidebar.header("关键词选择")

# 获取所有激活的关键词
try:
    all_keywords = list(keywords_collection.find({}, {"_id": 0, "keyword": 1, "active": 1}).sort("keyword", 1))
except Exception as e:
    st.sidebar.error(f"获取关键词失败: {e}")
    all_keywords = []

active_keywords = [kw['keyword'] for kw in all_keywords if kw.get('active', True)]

if active_keywords:
    # 获取当前用户的 selected_keywords
    try:
        user_pref = preferences_collection.find_one({"user_id": user_id})
        if user_pref and "selected_keywords" in user_pref:
            selected_keywords = user_pref["selected_keywords"]
        else:
            selected_keywords = []
    except Exception as e:
        st.sidebar.error(f"加载用户偏好失败: {e}")
        selected_keywords = []

    # 确保 selected_keywords 仅包含激活的关键词
    selected_keywords = [kw for kw in selected_keywords if kw in active_keywords]

    # 侧边栏多选框
    selected_keywords_sidebar = st.sidebar.multiselect(
        "选择要监控的关键词",
        options=active_keywords,
        default=selected_keywords,
        key='keyword_multiselect_sidebar'
    )

    # 处理“保存选择”按钮点击
    if st.sidebar.button("保存选择", key='save_selection_sidebar'):
        try:
            preferences_collection.update_one(
                {"user_id": user_id},
                {"$set": {"selected_keywords": selected_keywords_sidebar}},
                upsert=True
            )
            st.sidebar.success("选择已保存。")
            selected_keywords = selected_keywords_sidebar
        except Exception as e:
            st.sidebar.error(f"保存选择失败: {e}")
else:
    st.sidebar.warning("暂无激活的关键词。请在主页面添加并激活关键词。")

# -------------- 关键词管理（主页面） --------------
st.subheader("关键词管理")

if all_keywords:
    for kw in all_keywords:
        kw_name = kw['keyword']
        kw_active = kw.get('active', True)

        with st.expander(kw_name, expanded=False):
            col_toggle, col_delete = st.columns([1, 1])
            with col_toggle:
                toggle_label = "停用" if kw_active else "激活"
                if st.button(toggle_label, key=f"toggle_{kw_name}_{user_id}"):
                    try:
                        keywords_collection.update_one(
                            {"keyword": kw_name},
                            {"$set": {"active": not kw_active}}
                        )
                        st.success(f"关键词 '{kw_name}' 已{'停用' if kw_active else '激活'}。")
                        # 如果关键词被停用，从所有用户的 selected_keywords 中移除
                        if not kw_active:
                            preferences_collection.update_many(
                                {},
                                {"$pull": {"selected_keywords": kw_name}}
                            )
                        # 触发重新渲染
                        st.session_state['dummy'] = not st.session_state.get('dummy', False)
                    except Exception as e:
                        st.error(f"切换关键词状态失败: {e}")
            with col_delete:
                if st.button("删除", key=f"delete_{kw_name}_{user_id}"):
                    try:
                        keywords_collection.delete_one({"keyword": kw_name})
                        st.success(f"关键词 '{kw_name}' 已删除。")
                        # 从所有用户的 selected_keywords 中移除被删除的关键词
                        preferences_collection.update_many(
                            {},
                            {"$pull": {"selected_keywords": kw_name}}
                        )
                        # 触发重新渲染
                        st.session_state['dummy'] = not st.session_state.get('dummy', False)
                    except Exception as e:
                        st.error(f"删除关键词失败: {e}")
else:
    st.info("暂无关键词。请添加关键词。")

st.markdown("---")

# 新增关键词表单
st.subheader("新增关键词")
with st.form("add_keyword_form"):
    new_keyword = st.text_input("请输入新的关键词", "")
    submitted = st.form_submit_button("添加关键词")
    if submitted:
        if new_keyword.strip() == "":
            st.warning("关键词不能为空。")
        else:
            # 检查关键词是否已存在（忽略大小写）
            existing_keywords = [k['keyword'].lower() for k in all_keywords]
            if new_keyword.strip().lower() in existing_keywords:
                st.warning("该关键词已存在。")
            else:
                try:
                    keywords_collection.insert_one({"keyword": new_keyword.strip(), "active": True})
                    st.success(f"关键词 '{new_keyword.strip()}' 已成功添加。")
                    # 自动添加到当前用户的 selected_keywords
                    preferences_collection.update_one(
                        {"user_id": user_id},
                        {"$addToSet": {"selected_keywords": new_keyword.strip()}},
                        upsert=True
                    )
                    # 触发重新渲染
                    st.session_state['dummy'] = not st.session_state.get('dummy', False)
                except Exception as e:
                    st.error(f"添加关键词失败: {e}")

st.markdown("---")

# -------------- 构建查询条件 --------------
query = {}

if active_keywords and selected_keywords_sidebar:
    regex_pattern = '|'.join([re.escape(k) for k in selected_keywords_sidebar])
    query['$or'] = [
        {'type': 'submission', 'title': {'$regex': regex_pattern, '$options': 'i'}},
        {'type': 'submission', 'selftext': {'$regex': regex_pattern, '$options': 'i'}},
        {'type': 'comment', 'body': {'$regex': regex_pattern, '$options': 'i'}}
    ]

if start_date and end_date:
    start_datetime = datetime.combine(start_date, datetime.min.time()).replace(tzinfo=timezone.utc)
    end_datetime = datetime.combine(end_date, datetime.max.time()).replace(tzinfo=timezone.utc)
    query['created_at'] = {
        '$gte': start_datetime,
        '$lte': end_datetime
    }

if selected_subreddits and "全部" not in selected_subreddits:
    query['subreddit'] = {'$in': selected_subreddits}

# -------------- 显示查询条件 --------------
st.markdown("**当前查询条件:**")
st.json({
    "关键词过滤": selected_keywords_sidebar if selected_keywords_sidebar else "全部",
    "日期范围": {
        "开始": start_datetime.isoformat() if 'start_datetime' in locals() else "不限",
        "结束": end_datetime.isoformat() if 'end_datetime' in locals() else "不限"
    },
    "子版块过滤": selected_subreddits
})

# -------------- 分页计算 --------------
def calculate_total_pages(query, items_per_page):
    try:
        total_count = collection.count_documents(query)
        total_pages = (total_count + items_per_page - 1) // items_per_page
        return max(total_pages, 1)  # 至少1页
    except Exception as e:
        st.error(f"数据查询失败: {e}")
        return 1

items_per_page = st.slider("选择每页显示的帖子数量", min_value=10, max_value=100, value=20, step=10)

total_pages = calculate_total_pages(query, items_per_page)
st.session_state.total_pages = total_pages

# 调整当前页码，确保在有效范围内
if st.session_state.page_num > st.session_state.total_pages:
    st.session_state.page_num = st.session_state.total_pages
if st.session_state.page_num < 1:
    st.session_state.page_num = 1

# -------------- 查询数据 --------------
def get_posts(query, skip, limit):
    try:
        posts = list(collection.find(query).sort("created_at", -1).skip(skip).limit(limit))
        return posts
    except Exception as e:
        st.error(f"数据查询失败: {e}")
        return []

skip = (st.session_state.page_num - 1) * items_per_page
posts = get_posts(query, skip, items_per_page)

# -------------- 分页按钮 --------------
col_prev, col_mid, col_next = st.columns([1, 2, 1])
with col_prev:
    if st.button("上一页", key='previous_page'):
        previous_page()
with col_next:
    if st.button("下一页", key='next_page'):
        next_page()

# -------------- 显示页码信息 --------------
st.write(f"**页码**: {st.session_state.page_num} / {st.session_state.total_pages}")
st.write(f"**获取到的帖子数量**: {len(posts)}")
st.write(f"**跳过**: {skip}, **获取数量**: {items_per_page}")

# -------------- 显示帖子和评论 --------------
if not posts:
    st.info("暂无相关数据。请调整过滤条件或确保抓取脚本正在运行。")
else:
    # 转换为 DataFrame
    df = pd.DataFrame(posts)

    # 定义所有需要的列
    required_columns = ["type", "title", "selftext", "body", "score", "url", "link", "created_at", "author", "subreddit"]

    # 重新索引 DataFrame，确保所有列存在，缺失的列用空字符串填充
    df = df.reindex(columns=required_columns, fill_value='')

    # 使用 tabs 布局
    tabs = st.tabs(["列表视图", "数据表格", "可视化"])

    # 列表视图
    with tabs[0]:
        for _, post in df.iterrows():
            platform = post.get("type", "")
            created_at = post.get("created_at")
            author = post.get("author", "N/A")

            if platform == 'submission':
                title = post.get("title", "N/A")
                selftext = post.get("selftext", "")
                url = post.get("url", "#")
                st.markdown(f"### [{title}]({url}) | **分数**: {post.get('score')}")
                st.write(f"**作者**：{author} | **时间**：{created_at.strftime('%Y-%m-%d %H:%M:%S')} | **子版块**：{post.get('subreddit')}")
                if selftext:
                    st.markdown(selftext)
            elif platform == 'comment':
                body = post.get("body", "N/A")
                link = post.get("link", "#")
                st.markdown(f"### [评论]({link}) | **分数**: {post.get('score')}")
                st.write(f"**作者**：{author} | **时间**：{created_at.strftime('%Y-%m-%d %H:%M:%S')} | **子版块**：{post.get('subreddit')}")
                if body != "N/A":
                    st.markdown(body)
            else:
                st.write("未知类型。")

            st.markdown("---")

    # 数据表格视图
    with tabs[1]:
        st.subheader("数据表格视图")
        st.dataframe(df[required_columns])

    # 数据可视化视图
    with tabs[2]:
        st.subheader("子版块分布")
        try:
            subreddit_counts = df['subreddit'].value_counts().reset_index()
            subreddit_counts.columns = ['subreddit', 'counts']
            st.bar_chart(subreddit_counts.set_index('subreddit'))
        except Exception as e:
            st.error(f"无法生成子版块分布图: {e}")

# -------------- 保存cookies --------------
cookies.save()
