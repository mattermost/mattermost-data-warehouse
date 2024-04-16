from sentence_transformers import SentenceTransformer, util
import nltk
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer
import re
from collections import defaultdict
from difflib import get_close_matches
import numpy as np
import os
import requests
import snowflake.connector
from tabulate import tabulate
from dotenv import load_dotenv
import pandas as pd



load_dotenv()

conn = snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("SNOWFLAKE_TRANSFORM_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_TRANSFORM_DATABASE"),
        schema=os.getenv("SNOWFLAKE_TRANSFORM_SCHEMA"),
        role=os.getenv("SNOWFLAKE_TRANSFORM_ROLE"),
    )


# Sample topics and their associated keywords
topics = {
    "Invalid": ["invalid"],
     "Audio Calls & Screensharing": ["call", "calls", "calling", "audio call", "voice channel", "voice channels", "voice conference", "voice conferencing",
                                     "audio conference", "audio conferencing", "audio meeting", "screen sharing", "screen share", "screenshare", "microphone", "headphones", "audio quality", "voice quality", "discord"
                                     , "voice", "ring", "ringing", "voice connection"],
    "AD/LDAP": ["active directory", "ldap", "ad", "ad/ldap"],
    "Miscellanious Admin": ["admin", "admins", "administrate", "administrator", "administrators", "administrater", 
               "administraters", "administration", "administrating", "administrated", "system", "systems", 
               "config", "configuration", "configurating", "configurated", "configurates", "console", "system console"],
    "Reporting/Statistics": ["statistics", "rerpoting", "metrics", "site statistics", "team statistics", "active users", "chart", "graph"],
    "SSO": ["sso", "single sign on", "single-sign-on", "single sign-on", "open id connect", "open id", "openid", "saml", "oauth"],
    "System Console UX": ["system ux", "system ui", "system layout", "system design", "system experience", "system buttons", "system controls", "system control",
      "system adjust", "system set", "system enable", "system enabled", "system disable", "system disabled", "console ux", "console ui", "console layout", "console design", 
      "console experience", "console buttons", "console controls", "console control", "console adjust", "console set", "console enable", "console enabled", "console disable", "console disabled",
      "admin ux", "admin ui", "admin layout", "admin design", "admin experience", "admin buttons", "admin controls", 
      "admin control", "admin adjust", "admin set", "admin enable", "admin enabled", "admin disable", "admin disabled"],
    "Configuraiton/Customization": ["customize", "customization", "custom brand", "branding", "white label", "white labeling"],
    "User/Team Management": ["user management", "team management", "managing", "manage", "manages", "restrict", "restricted", 
                             "restricting", "access", "role", "roles", "group", "groups", "permission", "permissions"],
    "Compliance": ["compliant", "comply", "compliance"],
    "Video Calling": ["video call", "video calls", "video meeting", "video meetings", "video conference", "video conferencing", "video meet", "huddle", "webcam",],
    "Block User": ["ban", "banning", "banned", "block", "blocking", "blocked"],
    "Code Snippets": ["code", "coding", "snippet", "snippets", "syntax", "javascript", "html", "css", "scss", "typescript", "java", "php", "sql", "ruby"],
    "Corporate Slack Import": ["slack import", "import from slack", "import slack", "importing slack", "import from slack", "importing from slack"],
    "Notifications Inbox": ["notiifcations list", "notifications view", "view notifications", "view all notifications", "see all notifications", "list of notifications", "notification inbox", "all my notifications"],
    "Message Tagging": ["tag", "tags", "tagging", "tagged", "hashtag"],
    "Move Messages": ["move message", "moving message", "moving messages", "move between", "move from", "wrangler"],
    "Multiple Chat Windows": ["popup", "pop-up", "pop up", "popout", "pop-out", "pop out", "open window", "open windows", "new window", "new windows", "new chat", "new chats"
         "separate window", "separate windows", "multiple windows", "another window", "other window", "tabs", "multiple tabs", "separate tab", "separate tabs", "chat window", "chat windows"],
    "Read Receipts": ["receipt", "receipts", "mark read", "marking read", "marked read", "show read", "showing read", "message read", "message status",
          "message viewed", "message seen", "has been read", "has been seen", "message is read", "message was read", "has read my message"],
    "Scheduled Messages": ["send later", "sending later", "sent later", "schedule message", "scheduling message", "scheduled message", "scheduling a message", "scheduled messages", "scheduling messages", "schedule", "messages"],
    "Voice Messages": ["voice message", "voice messages", "audio message", "audio messages", "audio recording", "voice recording", "voice recordings", "voice memo", "voice memos", "voice communication", "voice text"],
    "Integration Requests": ["google", "calendar", "office", "drive", "confluence", "asana", "trello", "notion", "miro", "figma", "pagerduty", "addon", "add on"],
    "Jira": ["jira", "atlassian"],
    "GitHub": ["github"],
    "GitLab": ["gitlab"],
    "MS Teams": ["microsoft teams", "ms teams", "teams plugin", "ms teams plugin"],
    "Miscellaneous Integrations": ["integrate", "integrates", "integrated", "integration", "integrations", "plugin"],
    "ServiceNow": ["service now", "servicenow"],
    "Emotes/gifs": ["emotes", "emote", "gifs", "gif", "emoji", "smiley"],
    "Zoom": ["zoom"],
    "Emotes/gifs": ["emotes", "emote", "gifs", "gif", "emoji", "smiley"],
    "Praise": ["good", "i like", "great", "love", "awesome", "brilliant", "fantastic", "amazing", "happy", "seems fine", "10/10", 
               "perfect", "easy to use", "works well", "pleased", "pleasant", "exceptional", "remarkable", "delight", "delightful", "enjoy", "terrific", "phenomenal", "celebrate", "praise", "great app"],
    "Desktop Stability": ["desktop blank", "desktop white", "desktop freezes", "desktop freeze", "desktop freezing", "desktop frozen", "desktop crash", 
                          "desktop crashing", "desktop crashed", "desktop crashes", "desktop bug", "desktop bugs", "desktop buggy", "desktop stable", "desktop unstable", "desktop unreliable", 
                          "desktop reliable", "desktop reliability", "desktop not working", "desktop working", "desktop fail", "desktop fails", "desktop fix", "desktop fixed", "desktop fixes", "desktop support", 
                          "desktop supported", "desktop loading", "desktop loads", "desktop load", "desktop broken", "desktop break", "desktop breaks",
                          "windows blank", "windows white", "windows freezes", "windows freeze", "windows freezing", "windows frozen", "windows crash", 
                          "windows crashing", "windows crashed", "windows crashes", "windows bug", "windows bugs", "windows buggy", "windows stable", "windows unstable", "windows unreliable", 
                          "windows reliable", "windows reliability", "windows not working", "windows working", "windows fail", "windows fails", "windows fix", "windows fixed", "windows fixes", "windows support", 
                          "windows supported", "windows loading", "windows loads", "windows load", "windows broken", "windows break", "windows breaks",
                          "linux blank", "linux white", "linux freezes", "linux freeze", "linux freezing", "linux frozen", "linux crash", 
                          "linux crashing", "linux crashed", "linux crashes", "linux bug", "linux bugs", "linux buggy", "linux stable", "linux unstable", "linux unreliable", 
                          "linux reliable", "linux reliability", "linux not working", "linux working", "linux fail", "linux fails", "linux fix", "linux fixed", "linux fixes", "linux support", 
                          "linux supported", "linux loading", "linux loads", "linux load", "linux broken", "linux break", "linux breaks",
                          "ubuntu blank", "ubuntu white", "ubuntu freezes", "ubuntu freeze", "ubuntu freezing", "ubuntu frozen", "ubuntu crash", 
                          "ubuntu crashing", "ubuntu crashed", "ubuntu crashes", "ubuntu bug", "ubuntu bugs", "ubuntu buggy", "ubuntu stable", "ubuntu unstable", "ubuntu unreliable", 
                          "ubuntu reliable", "ubuntu reliability", "ubuntu not working", "ubuntu working", "ubuntu fail", "ubuntu fails", "ubuntu fix", "ubuntu fixed", "ubuntu fixes", "ubuntu support", 
                          "ubuntu supported", "ubuntu loading", "ubuntu loads", "ubuntu load", "ubuntu broken", "ubuntu break", "ubuntu breaks",
                          "mac blank", "mac white", "mac freezes", "mac freeze", "mac freezing", "mac frozen", "mac crash", 
                          "mac crashing", "mac crashed", "mac crashes", "mac bug", "mac bugs", "mac buggy", "mac stable", "mac unstable", "mac unreliable", 
                          "mac reliable", "mac reliability", "mac not working", "mac working", "mac fail", "mac fails", "mac fix", "mac fixed", "mac fixes", "mac support", 
                          "mac supported", "mac loading", "mac loads", "mac load", "mac broken", "mac break", "mac breaks",
                          "macosx blank", "macosx white", "macosx freezes", "macosx freeze", "macosx freezing", "macosx frozen", "macosx crash", 
                          "macosx crashing", "macosx crashed", "macosx crashes", "macosx bug", "macosx bugs", "macosx buggy", "macosx stable", "macosx unstable", "macosx unreliable", 
                          "macosx reliable", "macosx reliability", "macosx not working", "macosx working", "macosx fail", "macosx fails", "macosx fix", "macosx fixed", "macosx fixes", "macosx support", 
                          "macosx supported", "macosx loading", "macosx loads", "macosx load", "macosx broken", "macosx break", "macosx breaks"],
    "Message and Network Reliability": ["disconnect", "disconnected", "disconnects", "connects", "connection", "not sent", "not sending", "won't send", "can't send", "sync", "synchronization", "save", "saves", "saved"],
    "Miscellaneous Bugs": ["bug", "bugs", "buggy", "error", "errors", "crash", "crashes", "crashed", "glitch", "glitches", "inconvenient"],
    "Mobile Stability": ["mobile freezes", "mobile freeze", "mobile freezing", "mobile frozen", "mobile crash", "mobile crashing", "mobile crashed", "mobile crashes", "mobile bug", "mobile bugs", "mobile buggy", "mobile stable", "mobile unstable", "mobile unreliable", 
                         "mobile reliable", "mobile reliability", "mobile not working", "mobile working", "mobile fail", "mobile fails", "mobile fix", "mobile fixed", "mobile fixes", "mobile support", 
                         "mobile supported", "mobile loading", "mobile loads", "mobile load", "mobile problem", "mobile problems", "mobile broken", "mobile break", "mobile breaks",
                         "ios freezes", "ios freeze", "ios freezing", "ios frozen", "ios crash", "ios crashing", "ios crashed", "ios crashes", "ios bug", "ios bugs", "ios buggy", "ios stable", "ios unstable", "ios unreliable", "ios reliable", 
                         "ios reliability", "ios not working", "ios working", "ios fail", "ios fails", "ios fix", "ios fixed", "ios fixes", "ios support", "ios supported", 
                         "ios loading", "ios loads", "ios load", "ios problem", "ios problems", "ios broken", "ios break", "ios breaks", "iphone freezes", "iphone freeze", "iphone freezing", "iphone frozen", "iphone crash", "iphone crashing", 
                         "iphone crashed", "iphone crashes", "iphone bug", "iphone bugs", "iphone buggy", "iphone stable", "iphone unstable", "iphone unreliable", "iphone reliable", 
                         "iphone reliability", "iphone not working", "iphone working", "iphone fail", "iphone fails", "iphone fix", "iphone fixed", "iphone fixes", "iphone support", "iphone supported", "iphone loading", "iphone loads", 
                         "iphone load", "iphone problem", "iphone problems", "iphone broken", "iphone break", "iphone breaks", "android freezes", "android freeze", "android freezing", "android frozen", "android crash", "android crashing",
                         "android crashed", "android crashes", "android bug", "android bugs", "android buggy", "android stable", "android unstable", "android unreliable", "android reliable", "android reliability", 
                         "android not working", "android working", "android fail", "android fails", "android fix", "android fixed", "android fixes", "android support", "android supported", "android loading", "android loads", 
                         "android load", "android problem", "android problems", "android broken", "android break", "android breaks"],
    "Notifications Reliability": ["notification missing", "notification not working", "notification not getting", "notification not receiving", "notification failing", "notification fail", "notification fails", "notification reliable", "notification reliability", 
                                "notification unreliable", "notification doesn't work", "notification bad", "notification terrible", "notification fix", 
                                "notification deliver", "notification unable", "notification can't get", "notification broken", "notification busted", "notifying missing", "notifying not working", "notifying not getting", "notifying not receiving", "notifying failing", "notifying fail", 
                                "notifying fails", "notifying reliable", "notifying reliability", "notifying unreliable", "notifying doesn't work", "notifying bad", "notifying terrible", "notifying fix", "notifying deliver", "notifying unable", "notifying can't get", "notifying broken", "notifying busted", 
                                "notified missing", "notified not working", "notified not getting", "notified not receiving", "notified failing", "notified fail", "notified fails", "notified reliable", "notified reliability", "notified unreliable", "notified doesn't work", "notified bad", "notified terrible", 
                                "notified fix", "notified deliver", "notified unable", "notified can't get", "notified broken", "notified busted"],
    "Performance": ["slow", "loading", "performance", "performs", "performant", "perform", "cpu", "memory", "lag", "lagging", "fast", "faster", "long time", "speed", "speeds", "speedier", "snappy", "snappier", "snappiness"],
    "Authentication": ["access", "accessing", "accessed", "auth", "authentication", "authenticate", "authenticated", "authenticating", "log in", "login", "log-in", "logging in", "logged in", "sign in", 
                       "sign-in", "signing in", "signed in", "signup", "sign up", "sign-up", "sign on", "signon", "sign-on", "log out", "log-out", "logout", "logging out", "logged out", "log me out", 
                       "log me off", "logged me out", "logged me off", "logging me out", "logging me off", "password", "passwords", "username"],
    "Accessibility": ["accessibility", "disability", "disabilities", "wcag", "web standards", "screen reader", "assistive", "keyboard control", "keyboard shortcut", "shortcuts"],
    "Be Slack": ["slack"],
    "Boards": ["boards", "kanban", "focalboard"],
    "Channel Navigation": ["sidebar", "lhs", "left sidebar", "channels", "channel list", "channel sidebar", "find channel", "find channels", "finding channels", "unread", "unreads", "favorite", "favorites", "favoriting", "favorited", "favourite", "favourites", "favouriting", "favourited", "category", "categories",
                            "categorizing", "categorized", "layout", "navigation", "navigating", "nav", "navigated", "sort", "order", "organize", "organized", "organizing"],
    "Direct Message UX": ["desktop app", "desktop apps", "desktop application", "desktop standalone", "windows app", "windows apps", "windows application", "windows standalone",
                        "linux app", "linux apps", "linux application", "linux standalone", "ubuntu app", "ubuntu apps", "ubuntu application", "ubuntu standalone",
                        "mac app", "mac apps", "mac application", "mac standalone", "macosx app", "macosx apps", "macosx application", "macosx standalone", "electron app", "electron apps", "electron application", "electron standalone"],
    "Documentation/Training": ["guide", "guidance", "help", "training", "learning", "tips", "doc", "docs", "documentation", "how to"],
    "Group Message UX": ["group message", "gm", "group messaging", "group messages", "group chat", "group chats"],
    "Mark as Unread": ["unread", "mark unread", "mark read", "follow-up", "follow up", "read later"],
    "Messaging UX": ["send", "sending", "sent", "chat", "chats", "chatting", "chatted", "communicate", "communicates", "communicating", "talk", "talking", "format", "formats", "formatting", "formatted",
                      "markdown", "post", "posts", "message", "messages", "text", "texts", "bullet", "bulleted", "bullets", "typing", "key", "keys", "keystroke",
                      "keystrokes", "character", "characters", "wysiwyg", "what you see is what you get", "message preview", "edit", "editing", "drafts", "draft", "editor", "delete", "delete messages"],
    "Message Priority": ["priority", "high", "important", "urgent"],
    "Mobile UX": ["mobile experience", "mobile ui", "mobile ux", "mobile app", "mobile apps", "mobile application", "mobile applications", "mobile telegram", "mobile signal", "mobile whatsapp", "mobile imessage", "mobile messenger", "mobile improve", "mobile better", "mobile user", "mobile user experience", "mobile junk", "mobile garbage", "mobile terrible",
                "iphone experience", "iphone ui", "iphone ux", "iphone app", "iphone apps", "iphone application", "iphone applications", "iphone telegram", "iphone signal", "iphone whatsapp", "iphone imessage", "iphone messenger", "iphone improve", "iphone better", "iphone user", "iphone user experience", "iphone junk", "iphone garbage", "iphone terrible",
                "ios experience", "ios ui", "ios ux", "ios app", "ios apps", "ios application", "ios applications", "ios telegram", "ios signal", "ios whatsapp", "ios imessage", "ios messenger", "ios improve", "ios better", "ios user", "ios user experience", "ios junk", "ios garbage", "ios terrible",
                "android experience", "android ui", "android ux", "android app", "android apps", "android application", "android applications", "android telegram", "android signal", "android whatsapp", "android imessage", "android messenger", "android improve", "android better", "android user", "android user experience", "android junk", "android garbage", "android terrible"],
    "Mobile Multi-Server": ["mobile servers", "mobile instances", "mobile workspaces", "mobile several", "mobile multiple", "mobile multi-server", "mobile multi-workspace", "mobile multiple accounts",
                "iphone servers", "iphone instances", "iphone workspaces", "iphone several", "iphone multiple", "iphone multi-server", "iphone multi-workspace", "iphone multiple accounts",
                "ios servers", "ios instances", "ios workspaces", "ios several", "ios multiple", "ios multi-server", "ios multi-workspace", "ios multiple accounts",
                "android servers", "android instances", "android workspaces", "android several", "android multiple", "android multi-server", "android multi-workspace", "android multiple accounts"],
    "Notifications/Mentions UX": ["mention", "mentions", "notify", "notifying", "notifies", "notified", "notification", "notifications", "notices", "alerts", "alarms", "sounds", "mute", "bubble", "bubbles", "banner", "banners", "snooze"],
    "Onboarding": ["new user", "new users", "onboarding", "on board", "onboarded", "first time", "tutorial", "video tour", "tutorial video", "video tutorial", "video tutorials", "tutorial bot"],
    "Playbooks": ["playbooks", "playbook", "checklist", "tasks", "task list", "workflow", "procedure", "process"],
    "Reminderbot": ["remind", "reminds", "reminder", "reminders", "reminded", "reminding"],
    "Search UX": ["search", "searched", "searches", "searching", "find", "finding", "finds", "results", "found"],
    "User Settings": ["setting", "settings", "preference", "preferences"],
    "Slash Commands UX": ["slash", "command", "commands"],
    "Forward Message UX": ["share", "sharing", "shared", "forward", "forwarding", "forwarded"],
    "Themes": ["theme", "themes", "colors", "dark", "light", "bright", "contrast"],
    "Threads UX": ["thread", "threads", "threading", "threaded", "replies", "reply", "replying", "replied", "inbox", "following", "followed", "answer", "answers", "answering", "answered", "comment", "commenting", "commented"],
    "Translations": ["language", "translate", "translation", "translated", "local", "localization", "rtl", "right-to-left", "right to left", "english", "french", "spanish", "italian", "chinese", "russian", "arabic", "japanese", "hebrew",
                      "portuguese", "german", "dutch", "swedish", "hindi", "punjabi", "korean", "polish", "turkish", "vietnamese", "hungarian", "czech"],
    "UI/UX Polish": ["ui", "ux", "aesthetic", "aesthetics", "aesthetically", "visual", "interface", "gui", "graphics", "clutter", "clean", "noise", "icon", "icons",
                      "polish", "symbols", "font", "style", "alignment", "aligned", "misaligned", "space", "user-friendly", "user friendly", "intuitive", "clunky", "messy"],
    "User Groups": ["groups", "manage group", "manage groups"],
    "User Status": ["away", "online", "offline", "busy", "do not disturb", "dnd", "status", "set status", "setting status", "custom status", "customized status", "update status", "updating status", "updated status"],
}


# Load pre-trained model
model = SentenceTransformer('paraphrase-MiniLM-L6-v2')

# Preprocessing functions
lemmatizer = WordNetLemmatizer()
stop_words = set(stopwords.words('english'))

def preprocess_text(text):
    tokens = word_tokenize(re.sub(r'[^a-zA-Z0-9\s]', '', text.lower()))
    filtered_tokens = [lemmatizer.lemmatize(token) for token in tokens if token not in stop_words]
    return filtered_tokens

def embed_sentence(sentence):
    return model.encode(sentence, convert_to_tensor=True)

def cosine_similarity(u, v):
    return util.pytorch_cos_sim(u, v).item()

# Topic classification function with spelling mistake approximation
def cosine(sentence, threshold=0.8):
    preprocessed_sentence = preprocess_text(sentence)
    sentence_embedding = embed_sentence(" ".join(preprocessed_sentence))

    scores = defaultdict(int)
    
    for topic, keywords in topics.items():
        topic_embedding = embed_sentence(" ".join(keywords))
        sim_score = cosine_similarity(sentence_embedding, topic_embedding)
        scores[topic] = sim_score
    
    if not any(scores.values()):
        return "Unknown"  # If no category found, return "Unknown"
    
    max_score_topic = max(scores, key=scores.get)
    return max_score_topic

def fetch_and_classify_data():
    try:
        cur = conn.cursor()

        # Adjust your query as needed
        sql = '''
        select distinct server_id as server_id,
            user_id as user_id,
            feedback as feedback,
            feedback_date as feedback_date,
            server_version as server_version
        from mart_product.fct_nps_feedback nps
        where nps.feedback_DATE = CURRENT_DATE - 1
        order by server_id, user_id
        '''

        cur.execute(sql)
        rows = cur.fetchall()
        columns = [col[0] for col in cur.description]
        
        # Create DataFrame from fetched data
        df = pd.DataFrame(rows, columns=columns)
        print("Data fetched successfully.")
        
        # Classify each feedback entry and add it to a new column 'Category'
        df['CATEGORY'] = df['FEEDBACK'].apply(cosine)
        return df

    finally:
        cur.close()
        conn.close()

df_classified = fetch_and_classify_data()
print(df_classified)
