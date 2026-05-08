# bot.py – Optimised Telegram SMS Bot (Multi‑Number, Continuous Monitoring + Balance & Withdraw)
import warnings
warnings.filterwarnings("ignore", message=".*urllib3.*")
warnings.filterwarnings("ignore", category=DeprecationWarning)

import requests, time, re, random, os, json, logging, threading, pyotp, string, sqlite3
from datetime import datetime, timedelta
from html import escape, unescape
from collections import defaultdict
from dotenv import load_dotenv
from faker import Faker
from concurrent.futures import ThreadPoolExecutor
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ---------- Logging suppression ----------
logging.getLogger().setLevel(logging.CRITICAL)
for lname in ['urllib3', 'requests', 'faker', 'pyotp']:
    logging.getLogger(lname).setLevel(logging.CRITICAL)

load_dotenv()

# ---------- Environment ----------
TELEGRAM_TOKEN = os.getenv('TELEGRAM_TOKEN')
GROUP_IDS     = [gid.strip() for gid in os.getenv('GROUP_ID', '').split(',') if gid.strip()]
TIMEOUT_SECONDS = int(os.getenv('TIMEOUT_SECONDS', 600))
ADMIN_IDS     = [int(x.strip()) for x in os.getenv('ADMIN_IDS', '').split(',') if x.strip()]

DEFAULT_STEX_EMAIL    = os.getenv('STEX_EMAIL')
DEFAULT_STEX_PASSWORD = os.getenv('STEX_PASSWORD')
DEFAULT_MNIT_EMAIL    = os.getenv('MNIT_EMAIL')
DEFAULT_MNIT_PASSWORD = os.getenv('MNIT_PASSWORD')

if not TELEGRAM_TOKEN:
    raise EnvironmentError('TELEGRAM_TOKEN required')
if not DEFAULT_STEX_EMAIL or not DEFAULT_STEX_PASSWORD:
    raise EnvironmentError('STEX_EMAIL and STEX_PASSWORD required')
if not DEFAULT_MNIT_EMAIL or not DEFAULT_MNIT_PASSWORD:
    raise EnvironmentError('MNIT_EMAIL and MNIT_PASSWORD required')

TG_API = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}"
try:
    bot_info = requests.get(f"{TG_API}/getMe", timeout=10).json()
    BOT_USERNAME = bot_info['result']['username']
except Exception:
    BOT_USERNAME = None

# ---------- Database with WAL ----------
DB_FILE = os.environ.get('DB_PATH', 'user_creds.db')
db_dir = os.path.dirname(DB_FILE)
if db_dir and not os.path.exists(db_dir):
    os.makedirs(db_dir, exist_ok=True)

db_lock = threading.Lock()

def init_db():
    with db_lock:
        conn = sqlite3.connect(DB_FILE, check_same_thread=False)
        conn.execute('PRAGMA journal_mode=WAL')
        c = conn.cursor()
        # existing
        c.execute('''
            CREATE TABLE IF NOT EXISTS user_credentials (
                user_id   INTEGER,
                provider  TEXT,
                email     TEXT,
                password  TEXT,
                PRIMARY KEY (user_id, provider)
            )
        ''')
        # new: user balance & wallets
        c.execute('''
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                balance_bdt REAL DEFAULT 0.0,
                bkash TEXT,
                rocket TEXT,
                binance TEXT
            )
        ''')
        # withdraw requests
        c.execute('''
            CREATE TABLE IF NOT EXISTS withdraw_requests (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                amount_bdt REAL,
                method TEXT,
                wallet_detail TEXT,
                status TEXT DEFAULT 'pending',
                request_time TEXT,
                completed_time TEXT
            )
        ''')
        conn.commit()
        conn.close()
init_db()

# ---------- Wallet / Balance helpers (thread‑safe) ----------
def ensure_user_exists(user_id):
    with db_lock:
        conn = sqlite3.connect(DB_FILE)
        conn.execute('INSERT OR IGNORE INTO users (user_id) VALUES (?)', (user_id,))
        conn.commit()
        conn.close()

def get_user_balance(user_id):
    ensure_user_exists(user_id)
    with db_lock:
        conn = sqlite3.connect(DB_FILE)
        row = conn.execute('SELECT balance_bdt FROM users WHERE user_id = ?', (user_id,)).fetchone()
        conn.close()
        return row[0] if row else 0.0

def credit_user(user_id, amount_bdt):
    ensure_user_exists(user_id)
    with db_lock:
        conn = sqlite3.connect(DB_FILE)
        conn.execute('UPDATE users SET balance_bdt = balance_bdt + ? WHERE user_id = ?', (amount_bdt, user_id))
        conn.commit()
        conn.close()

def deduct_user(user_id, amount_bdt):
    ensure_user_exists(user_id)
    with db_lock:
        conn = sqlite3.connect(DB_FILE)
        conn.execute('UPDATE users SET balance_bdt = balance_bdt - ? WHERE user_id = ?', (amount_bdt, user_id))
        conn.commit()
        conn.close()

def get_user_wallet(user_id):
    with db_lock:
        conn = sqlite3.connect(DB_FILE)
        row = conn.execute('SELECT bkash, rocket, binance FROM users WHERE user_id = ?', (user_id,)).fetchone()
        conn.close()
        if row:
            return {'bkash': row[0], 'rocket': row[1], 'binance': row[2]}
        return {'bkash': None, 'rocket': None, 'binance': None}

def set_wallet_detail(user_id, field, value):
    ensure_user_exists(user_id)
    with db_lock:
        conn = sqlite3.connect(DB_FILE)
        conn.execute(f'UPDATE users SET {field} = ? WHERE user_id = ?', (value, user_id))
        conn.commit()
        conn.close()

def create_withdrawal(user_id, amount, method, wallet_detail):
    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    with db_lock:
        conn = sqlite3.connect(DB_FILE)
        # check sufficient balance (balance - sum of pending requests)
        pending_sum = conn.execute(
            'SELECT COALESCE(SUM(amount_bdt), 0) FROM withdraw_requests WHERE user_id = ? AND status = "pending"',
            (user_id,)
        ).fetchone()[0]
        balance = conn.execute('SELECT balance_bdt FROM users WHERE user_id = ?', (user_id,)).fetchone()[0]
        if balance - pending_sum < amount:
            conn.close()
            return False, "Insufficient balance (after pending requests)."
        conn.execute(
            'INSERT INTO withdraw_requests (user_id, amount_bdt, method, wallet_detail, status, request_time) VALUES (?, ?, ?, ?, "pending", ?)',
            (user_id, amount, method, wallet_detail, now)
        )
        conn.commit()
        conn.close()
        return True, None

def get_pending_requests():
    with db_lock:
        conn = sqlite3.connect(DB_FILE)
        rows = conn.execute(
            "SELECT id, user_id, amount_bdt, method, wallet_detail, request_time FROM withdraw_requests WHERE status='pending' ORDER BY request_time"
        ).fetchall()
        conn.close()
        return [
            {'id': r[0], 'user_id': r[1], 'amount_bdt': r[2], 'method': r[3], 'wallet_detail': r[4], 'time': r[5]}
            for r in rows
        ]

def complete_withdrawal(request_id, admin_id):
    with db_lock:
        conn = sqlite3.connect(DB_FILE)
        row = conn.execute('SELECT id, user_id, amount_bdt, method, wallet_detail FROM withdraw_requests WHERE id=? AND status="pending"', (request_id,)).fetchone()
        if not row:
            conn.close()
            return None
        user_id = row[1]
        amount = row[2]
        method = row[3]
        wallet = row[4]
        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        conn.execute('UPDATE withdraw_requests SET status="completed", completed_time=? WHERE id=?', (now, request_id))
        conn.execute('UPDATE users SET balance_bdt = balance_bdt - ? WHERE user_id = ?', (amount, user_id))
        conn.commit()
        conn.close()
        # notify user
        ex_rate = 125.0
        if method in ('bkash', 'rocket'):
            amount_display = f"{amount:.2f} BDT"
        else:  # binance, mobile_recharge
            amount_display = f"${amount/ex_rate:.4f}"
        msg = (
            f"🎉 <b>Withdrawal Approved</b>\n\n"
            f"💵 <b>Amount:</b> {amount_display}\n"
            f"🏦 <b>Method:</b> {method}\n"
            f"📞 <b>Wallet:</b> {wallet}\n"
            f"✅ <b>Status:</b> Complete\n\n"
            f"We appreciate your trust! Share your experience or reach support below."
        )
        tg_send(user_id, msg)
        return user_id

def get_withdrawal_history(user_id=None):
    """If user_id is None -> admin sees all completed, else user's completed."""
    with db_lock:
        conn = sqlite3.connect(DB_FILE)
        if user_id is None:
            rows = conn.execute(
                "SELECT id, user_id, amount_bdt, method, wallet_detail, request_time, completed_time FROM withdraw_requests WHERE status='completed' ORDER BY completed_time DESC"
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT id, user_id, amount_bdt, method, wallet_detail, request_time, completed_time FROM withdraw_requests WHERE user_id=? AND status='completed' ORDER BY completed_time DESC",
                (user_id,)
            ).fetchall()
        conn.close()
        return [
            {'id': r[0], 'user_id': r[1], 'amount_bdt': r[2], 'method': r[3], 'wallet': r[4], 'request_time': r[5], 'completed_time': r[6]}
            for r in rows
        ]

def is_admin(user_id):
    return user_id in ADMIN_IDS

# ---------- Credentials helpers (original) ----------
def save_credentials(user_id, provider, email, password):
    with db_lock:
        conn = sqlite3.connect(DB_FILE, check_same_thread=False)
        c = conn.cursor()
        c.execute('INSERT OR REPLACE INTO user_credentials (user_id, provider, email, password) VALUES (?, ?, ?, ?)',
                  (user_id, provider, email, password))
        conn.commit()
        conn.close()

def get_credentials(user_id, provider):
    with db_lock:
        conn = sqlite3.connect(DB_FILE, check_same_thread=False)
        c = conn.cursor()
        c.execute('SELECT email, password FROM user_credentials WHERE user_id=? AND provider=?', (user_id, provider))
        row = c.fetchone()
        conn.close()
        return (row[0], row[1]) if row else (None, None)

def delete_credentials(user_id, provider=None):
    with db_lock:
        conn = sqlite3.connect(DB_FILE, check_same_thread=False)
        c = conn.cursor()
        if provider:
            c.execute('DELETE FROM user_credentials WHERE user_id=? AND provider=?', (user_id, provider))
        else:
            c.execute('DELETE FROM user_credentials WHERE user_id=?', (user_id,))
        conn.commit()
        conn.close()

# ---------- Rate limit, states, globals ----------
bot_instances    = {}
instances_lock   = threading.RLock()
user_states      = {}
states_lock      = threading.RLock()
user_last_request = defaultdict(float)
user_latest_range = {}
user_latest_provider = {}

MAX_ACTIVE_NUMBERS = 5
active_monitors = {}
monitors_lock = threading.RLock()
executor = ThreadPoolExecutor(max_workers=50)
fake = Faker('en_US')

RATE_LIMIT_SECONDS = 10
EXCHANGE_RATE = 125.0

OTP_PATTERN = re.compile(
    r'<#>\s*(\d[\d\s-]{2,7}\d)\b|'
    r'(?:code|otp|pin|verification)[:\s]+(\d[\d\s-]{2,7}\d)\b|'
    r'(\d[\d\s-]{2,7}\d)\s+is\s+your|'
    r'([A-Z]{2,3}-\d+)|'
    r'\b(\d{4,8})\b',
    re.IGNORECASE
)

def extract_otp_universal(text: str):
    if not text:
        return None
    match = OTP_PATTERN.search(text)
    if match:
        for group in match.groups():
            if group:
                code = re.sub(r'[\s-]', '', group)
                if code.isdigit() and 3 <= len(code) <= 8:
                    return code
    return None

AVAILABLE_DOMAINS = [
    "mailto.plus","fexpost.com","fexbox.org","mailbox.in.ua",
    "rover.info","chitthi.in","fextemp.com","any.pink","merepost.com"
]
MAX_EMAILS = 5
INACTIVE_TIMEOUT = 30 * 60
FETCH_INTERVAL = 2

user_temp_emails = {}
temp_email_lock = threading.RLock()

def clean_number(number):
    return number.lstrip('+').strip() if number else number

def generate_strong_password():
    special_chars = "!@#$%^&*"
    chars = string.ascii_letters + string.digits + special_chars
    password_length = random.randint(10, 12)
    password = ''.join(random.choice(chars) for _ in range(password_length))
    bdt_time = datetime.now() + timedelta(hours=6)
    password += str(bdt_time.day)
    return password

def generate_identity(gender):
    if gender == 'male':
        first_name = fake.first_name_male()
        last_name = fake.last_name()
        emoji = '👨'
    else:
        first_name = fake.first_name_female()
        last_name = fake.last_name()
        emoji = '👩'
    full_name = f"{first_name} {last_name}"
    username = f"{first_name.lower()}{last_name.lower()}{random.randint(10,99)}"
    password = generate_strong_password()
    return emoji, full_name, username, password

def generate_temp_email(domain):
    local = ''.join(random.choices('abcdefghijklmnopqrstuvwxyz0123456789', k=10))
    return f"{local}@{domain}"

def clean_html(raw_html):
    raw_html = re.sub(r"<(script|style).*?>.*?</\1>", "", raw_html, flags=re.S)
    raw_html = re.sub(r"<br\s*/?>|</p>", "\n", raw_html)
    raw_html = re.sub(r"<[^>]+>", "", raw_html)
    raw_html = unescape(raw_html)
    return re.sub(r"\n{2,}", "\n", raw_html).strip()

def extract_otp_temp(text):
    return extract_otp_universal(text)

def fetch_latest_mail(email):
    encoded = email.replace("@", "%40")
    url = f"https://tempmail.plus/api/mails?email={encoded}&first_id=0&epin="
    headers = {
        "X-Requested-With": "XMLHttpRequest",
        "Cookie": f"email={encoded}",
        "Referer": "https://tempmail.plus/en/"
    }
    try:
        r = requests.get(url, headers=headers, timeout=10)
        if r.status_code != 200:
            return None
        mails = r.json().get("mail_list", [])
        return mails[0] if mails else None
    except Exception:
        return None

def fetch_mail_content(email, mail_id):
    url = f"https://tempmail.plus/api/mails/{mail_id}"
    headers = {
        "X-Requested-With": "XMLHttpRequest",
        "Cookie": f"email={email.replace('@','%40')}",
        "Referer": "https://tempmail.plus/en/"
    }
    try:
        r = requests.get(url, headers=headers, timeout=10)
        if r.status_code != 200:
            return ""
        data = r.json()
        if data.get("text"):
            return data["text"].strip()
        if data.get("html"):
            return clean_html(data["html"])
        return ""
    except Exception:
        return ""

# ---------- StexSMS Class ----------
class StexSMS:
    def __init__(self, provider, email, password):
        self.provider = provider
        self.email = email
        self.password = password
        self.base = 'https://x.mnitnetwork.com' if provider == 'mnitnetwork' else 'https://stexsms.com'
        self.use_headers = (provider == 'mnitnetwork')
        self.session = self._create_session()
        self.token = None
        self.token_time = None
        self.TOKEN_TTL = 3600
        self._lock = threading.RLock()
        self._range_cache = {'data': None, 'timestamp': 0}

    def _create_session(self):
        session = requests.Session()
        retry = Retry(
            total=2,
            backoff_factor=0.5,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET", "POST"]
        )
        adapter = HTTPAdapter(
            pool_connections=50,
            pool_maxsize=50,
            max_retries=retry,
            pool_block=False
        )
        session.mount('https://', adapter)
        session.mount('http://', adapter)
        return session

    def _headers(self):
        h = {'Mauthtoken': self.token}
        if self.use_headers:
            h.update({
                'User-Agent': 'Mozilla/5.0',
                'Content-Type': 'application/json',
                'Accept-Encoding': 'gzip, deflate',
                'Connection': 'keep-alive'
            })
        return h

    def ensure_auth(self):
        with self._lock:
            if self.token is None or time.time() - self.token_time > self.TOKEN_TTL:
                self.login()

    def login(self):
        url = f"{self.base}/mapi/v1/mauth/login"
        payload = {'email': self.email, 'password': self.password}
        headers = {'User-Agent': 'Mozilla/5.0'} if self.use_headers else None
        for attempt in range(3):
            try:
                response = self.session.post(url, json=payload, headers=headers, timeout=15)
                response.raise_for_status()
                data = response.json()
                self.token = (data.get('token') or
                              data.get('access_token') or
                              data.get('data', {}).get('token') or
                              self.session.cookies.get('mauthtoken'))
                if not self.token:
                    raise RuntimeError('No token in response')
                self.token_time = time.time()
                return
            except (requests.Timeout, requests.ConnectionError) as e:
                if attempt == 2:
                    raise RuntimeError(f"Login failed after retries: {e}")
                time.sleep(0.5)
            except Exception as e:
                raise RuntimeError(f"Login error: {e}")

    def _request(self, method, url, **kwargs):
        self.ensure_auth()
        kwargs.setdefault('headers', self._headers())
        kwargs.setdefault('timeout', 20)
        for attempt in range(2):
            try:
                response = self.session.request(method, url, **kwargs)
                if response.status_code == 200:
                    return response
                elif response.status_code == 401 and attempt == 0:
                    with self._lock:
                        self.token = None
                        self.token_time = None
                    self.ensure_auth()
                    kwargs['headers'] = self._headers()
                    continue
                elif response.status_code == 429:
                    time.sleep(1)
                    continue
                response.raise_for_status()
            except (requests.Timeout, requests.ConnectionError):
                if attempt == 1:
                    raise
                time.sleep(0.5)
        return response

    def get_random_range(self):
        now = time.time()
        if self._range_cache['data'] and now - self._range_cache['timestamp'] < 300:
            return self._range_cache['data']
        response = self._request('GET', f"{self.base}/mapi/v1/mdashboard/console/info")
        logs = response.json().get('data', {}).get('logs', [])
        ranges = [log['number'] for log in logs if 'XXX' in log.get('number', '')]
        if not ranges:
            raise RuntimeError('No XXX ranges available')
        chosen = random.choice(ranges)
        self._range_cache = {'data': chosen, 'timestamp': now}
        return chosen

    def get_number_with_range(self, phone_range):
        response = self._request('POST', f"{self.base}/mapi/v1/mdashboard/getnum/number",
                                 json={'range': phone_range})
        raw = response.json()['data']['number']
        return clean_number(raw)

    def get_number(self):
        return self.get_number_with_range(self.get_random_range())

    def get_numbers_info(self, search=''):
        params = {'date': datetime.now().strftime('%Y-%m-%d'), 'page': 1, 'search': '', 'status': ''}
        response = self._request('GET', f"{self.base}/mapi/v1/mdashboard/getnum/info", params=params)
        numbers = response.json().get('data', {}).get('numbers', [])
        if search and isinstance(numbers, list):
            search_clean = clean_number(search)
            return [n for n in numbers if clean_number(n.get('number', '')) == search_clean]
        return numbers if isinstance(numbers, list) else []

    def extract_otp(self, text):
        return extract_otp_universal(text)

# ---------- Bot instance manager ----------
def get_bot_instance(provider, user_id=None):
    cache_key = (provider, user_id) if user_id else (provider, 'default')
    with instances_lock:
        if cache_key in bot_instances:
            return bot_instances[cache_key]
        if user_id:
            email, password = get_credentials(user_id, provider)
            if not email or not password:
                if provider == 'stexsms':
                    email, password = DEFAULT_STEX_EMAIL, DEFAULT_STEX_PASSWORD
                else:
                    email, password = DEFAULT_MNIT_EMAIL, DEFAULT_MNIT_PASSWORD
        else:
            if provider == 'stexsms':
                email, password = DEFAULT_STEX_EMAIL, DEFAULT_STEX_PASSWORD
            else:
                email, password = DEFAULT_MNIT_EMAIL, DEFAULT_MNIT_PASSWORD
        bot = StexSMS(provider=provider, email=email, password=password)
        bot.login()
        bot_instances[cache_key] = bot
        return bot

def logout_user(user_id, provider=None):
    delete_credentials(user_id, provider)
    with instances_lock:
        keys_to_del = [k for k in bot_instances if k[0] == provider and k[1] == user_id] if provider else \
                      [k for k in bot_instances if k[1] == user_id]
        for k in keys_to_del:
            del bot_instances[k]

def warmup_default_bots():
    try:
        get_bot_instance('stexsms')
        get_bot_instance('mnitnetwork')
    except Exception as e:
        print(f"Warmup warning: {e}")

def check_rate_limit(chat_id):
    now = time.time()
    last = user_last_request[chat_id]
    if now - last < RATE_LIMIT_SECONDS:
        return False, int(RATE_LIMIT_SECONDS - (now - last))
    user_last_request[chat_id] = now
    return True, 0

def validate_range(range_str):
    return bool(range_str and 'XXX' in range_str and re.match(r'^[\dX]+$', range_str))

# ---------- Keyboards ----------
def main_keyboard(user_id):
    has_creds = any(get_credentials(user_id, p)[0] for p in ['stexsms', 'mnitnetwork'])
    login_text = '🔓 Logout' if has_creds else '🔐 Log IN'
    keyboard = [
        [{'text': '📞 Get Number'}, {'text': '🔄 Change Number'}],
        [{'text': '👤 Fake Name'}, {'text': '🔐 Get 2FA'}],
        [{'text': login_text}, {'text': '📧 Temp Mail'}],
        [{'text': '👤 My Profile'}]
    ]
    return {'keyboard': keyboard, 'resize_keyboard': True}

def profile_keyboard(user_id):
    kb = [
        [{'text': '💰 Balance'}],
        [{'text': '📋 Withdraw History'}]
    ]
    if is_admin(user_id):
        kb.insert(1, [{'text': '📋 Withdraw List'}])   # admin only
    kb.append([{'text': '⬅️ Back'}])
    return {'keyboard': kb, 'resize_keyboard': True}

def gender_keyboard():
    return {'keyboard': [[{'text': '👨 Male'}, {'text': '👩 Female'}], [{'text': '⬅️ Back'}]], 'resize_keyboard': True}

def provider_keyboard():
    return {'keyboard': [[{'text': '🌐 StexSMS'}, {'text': '🌐 MNIT Network'}], [{'text': '⬅️ Back'}]], 'resize_keyboard': True}

def range_mode_keyboard():
    return {'keyboard': [[{'text': '🎲 Random Range'}, {'text': '✏️ Manual Range'}], [{'text': '⬅️ Back'}]], 'resize_keyboard': True}

def number_options_keyboard(number):
    return {'inline_keyboard': [[{'text': 'OTP Group ↗️', 'url': 'https://t.me/otpservers'}]]}

def group_message_keyboard():
    if not BOT_USERNAME:
        return None
    return {'inline_keyboard': [[{'text': '🚀 Get Number', 'url': f'https://t.me/{BOT_USERNAME}?start=main'}]]}

def login_provider_keyboard():
    return {'keyboard': [[{'text': '🌐 StexSMS'}, {'text': '🌐 MNIT Network'}], [{'text': '⬅️ Cancel'}]], 'resize_keyboard': True}

def cancel_keyboard():
    return {'keyboard': [[{'text': '⬅️ Cancel'}]], 'resize_keyboard': True}

def temp_domain_keyboard():
    rows, row = [], []
    for d in AVAILABLE_DOMAINS:
        row.append({'text': d})
        if len(row) == 2:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    rows.append([{'text': '⬅️ Cancel'}])
    return {'keyboard': rows, 'resize_keyboard': True}

def wallet_method_keyboard():
    return {
        'inline_keyboard': [
            [{'text': 'Bkash', 'callback_data': 'wallet_bkash'},
             {'text': 'Rocket', 'callback_data': 'wallet_rocket'},
             {'text': 'Binance', 'callback_data': 'wallet_binance'}]
        ]
    }

def withdraw_method_keyboard():
    return {
        'inline_keyboard': [
            [{'text': 'Bkash', 'callback_data': 'withdraw_method_bkash'},
             {'text': 'Rocket', 'callback_data': 'withdraw_method_rocket'},
             {'text': 'Binance', 'callback_data': 'withdraw_method_binance'},
             {'text': 'Mobile Recharge', 'callback_data': 'withdraw_method_mobile'}]
        ]
    }

# ---------- Message formatters ----------
def format_balance_message(user_id):
    balance = get_user_balance(user_id)
    wallet = get_user_wallet(user_id)
    usd = balance / EXCHANGE_RATE
    text = (
        "⚠️ <b>Double‑check your wallet! Wrong details = no refund.</b>\n\n"
        f"🤑 <b>Balance:</b> {balance:.2f} BDT / ${usd:.4f}\n\n"
        f"🌍 <b>Bkash:</b> {wallet['bkash'] or 'Not Set'}\n"
        f"🌍 <b>Rocket:</b> {wallet['rocket'] or 'Not Set'}\n"
        f"🌍 <b>Binance:</b> {wallet['binance'] or 'Not Set'}\n\n"
        f"💳 <b>Minimum Withdrawal:</b> 20 BDT / $0.50"
    )
    inline_kb = {
        'inline_keyboard': [
            [{'text': 'Set wallet', 'callback_data': 'profile_set_wallet'},
             {'text': 'Withdraw', 'callback_data': 'profile_withdraw'}]
        ]
    }
    return text, inline_kb

def format_inbox_message(number, provider, full_message, otp):
    t = datetime.now().strftime('%I:%M %p')
    msg = f"📩 <b>Message Received!</b>\n\n📞 <b>Number:</b> <code>+{number}</code>\n🏢 <b>Provider:</b> <code>{provider.upper()}</code>\n"
    if otp:
        msg += f"🔑 <b>OTP Code:</b> <code>{otp}</code>\n"
    msg += f"\n💬 <b>Full Message:</b>\n<blockquote>{escape(full_message)}</blockquote>\n\n🕒 <b>Time:</b> {t}"
    return msg

def format_failed_message(number, provider):
    t = datetime.now().strftime('%I:%M %p')
    return f"""❌ <b>Number Failed!</b>

📞 <b>Number:</b> <code>+{number}</code>
🏢 <b>Provider:</b> <code>{provider.upper()}</code>

This number can't receive SMS. Try again.

🕒 <b>Time:</b> {t}"""

def format_group_message(number, provider, full_message, otp):
    t = datetime.now().strftime('%I:%M %p')
    masked = f"{number[:3]}****{number[-3:]}" if len(number) > 6 else 'Unknown'
    msg = f"✅ <b>New message received!</b>\n\n📞 <b>Number:</b> <code>+{masked}</code>\n🏢 <b>Provider:</b> <code>{provider.upper()}</code>\n"
    if otp:
        msg += f"🔑 <b>OTP:</b> <code>{otp}</code>\n"
    msg += f"\n💬 <b>Message:</b>\n<blockquote>{escape(full_message)}</blockquote>\n\n🕒 <b>Time:</b> {t}"
    return msg

def format_identity_message(gender):
    emoji, full_name, username, password = generate_identity(gender)
    return f"""{emoji} <b>Generated Identity:</b>

Name : <code>{full_name}</code>
Username : <code>{username}</code>
Password : <code>{password}</code>

<i>Tap on the text above to copy</i>"""

def format_2fa_code(secret_key):
    try:
        clean_secret = ''.join(secret_key.split()).upper()
        totp = pyotp.TOTP(clean_secret)
        code = totp.now()
        time_remaining = 30 - (int(time.time()) % 30)
        msg = f"""🔐 <b>2FA Authentication Code</b>

Your Code : <code>{code}</code>

⏱ Valid for: <b>{time_remaining} seconds</b>

📌 <b>Note:</b> This code refreshes every 30 seconds."""
        return msg, True
    except Exception:
        return "❌ <b>Invalid Secret Key!</b>\n\nCheck format and try again.", False

def tg_send(chat_id, text, keyboard=None, parse_mode='HTML'):
    if not chat_id:
        return
    data = {'chat_id': chat_id, 'text': text, 'parse_mode': parse_mode}
    if keyboard:
        data['reply_markup'] = json.dumps(keyboard)
    try:
        requests.post(f"{TG_API}/sendMessage", data=data, timeout=5)
    except Exception:
        pass

def send_to_all_groups(text, keyboard=None):
    for gid in GROUP_IDS:
        tg_send(gid, text, keyboard)

# ---------- Multi‑number monitoring (balance‑aware) ----------
def monitor_number_loop(chat_id, number, provider_name, range_used, start_time):
    cancel_evt = None
    try:
        bot = get_bot_instance(provider_name, user_id=chat_id)
        with monitors_lock:
            if chat_id not in active_monitors:
                active_monitors[chat_id] = {}
            if number in active_monitors[chat_id]:
                return
            cancel_evt = threading.Event()
            active_monitors[chat_id][number] = {
                'future': None,
                'cancel': cancel_evt,
                'provider': provider_name,
                'start': start_time
            }

        last_msg_text = ""
        timeout = TIMEOUT_SECONDS
        failed = False
        # Determine if user is using default account (no custom credentials)
        custom_email, _ = get_credentials(chat_id, provider_name)
        using_default = (custom_email is None)

        while time.time() - start_time < timeout and not cancel_evt.is_set():
            try:
                nums = bot.get_numbers_info(search=number)
                for n in nums:
                    if clean_number(n.get('number', '')) != number:
                        continue
                    status = n.get('status', '')
                    msg = n.get('message') or n.get('otp') or ''
                    if status == 'failed':
                        failed = True
                        break
                    if status == 'success' and msg and msg != last_msg_text:
                        last_msg_text = msg
                        otp = bot.extract_otp(msg)
                        tg_send(chat_id, format_inbox_message(number, provider_name, msg, otp), number_options_keyboard(number))
                        if otp and GROUP_IDS:
                            send_to_all_groups(format_group_message(number, provider_name, msg, otp), group_message_keyboard())
                        # Credit user if default account
                        if using_default:
                            credit_user(chat_id, 0.30)
                if failed:
                    break
            except Exception:
                pass

            elapsed = time.time() - start_time
            sleep_time = 1.5 if elapsed < 15 else (2 if elapsed < 45 else (3 if elapsed < 90 else 4))
            if cancel_evt.wait(sleep_time):
                break

        if failed:
            tg_send(chat_id, format_failed_message(number, provider_name))

    except Exception as e:
        tg_send(chat_id, f"❌ Monitoring error for +{number}: {escape(str(e))}")
    finally:
        with monitors_lock:
            if chat_id in active_monitors and number in active_monitors[chat_id]:
                del active_monitors[chat_id][number]
                if not active_monitors[chat_id]:
                    del active_monitors[chat_id]

def start_number_monitoring(chat_id, number, provider_name, range_used):
    with monitors_lock:
        if chat_id not in active_monitors:
            active_monitors[chat_id] = {}
        if len(active_monitors[chat_id]) >= MAX_ACTIVE_NUMBERS:
            tg_send(chat_id, f"❌ You already have {MAX_ACTIVE_NUMBERS} numbers being monitored.", main_keyboard(chat_id))
            return
        if number in active_monitors[chat_id]:
            return

    future = executor.submit(monitor_number_loop, chat_id, number, provider_name, range_used, time.time())
    with monitors_lock:
        if chat_id in active_monitors and number in active_monitors[chat_id]:
            active_monitors[chat_id][number]['future'] = future

def handle_create_number(provider, chat_id, manual_range=None):
    try:
        allowed, remaining = check_rate_limit(chat_id)
        if not allowed:
            tg_send(chat_id, f"⏳ Please wait {remaining}s.", main_keyboard(chat_id))
            return

        with monitors_lock:
            if len(active_monitors.get(chat_id, {})) >= MAX_ACTIVE_NUMBERS:
                tg_send(chat_id, f"❌ Max {MAX_ACTIVE_NUMBERS} numbers monitored.", main_keyboard(chat_id))
                return

        bot = get_bot_instance(provider, user_id=chat_id)

        if manual_range:
            number = bot.get_number_with_range(manual_range)
            range_info = f"\n📋 <b>Range:</b> <code>{escape(manual_range)}</code>"
            with states_lock:
                user_latest_range[chat_id] = manual_range
                user_latest_provider[chat_id] = provider
        else:
            number = bot.get_number()
            range_info = ''

        timeout_minutes = TIMEOUT_SECONDS // 60
        tg_send(chat_id, f"{range_info}\n\n📞 <b>Your number:</b> <code>+{number}</code>\n\n🔄 Monitoring started – all messages within {timeout_minutes} mins.",
                number_options_keyboard(number))
        start_number_monitoring(chat_id, number, provider, manual_range if manual_range else 'Random')

    except Exception as e:
        tg_send(chat_id, f"❌ Error: {escape(str(e))}", main_keyboard(chat_id))

# ---------- Login flow (unchanged) ----------
def start_login(chat_id):
    with states_lock:
        user_states[chat_id] = {'step': 'awaiting_login_provider'}
    tg_send(chat_id, "🔐 <b>Select provider to log in:</b>", login_provider_keyboard())

def process_login_provider(chat_id, text):
    if text == '⬅️ Cancel':
        with states_lock:
            user_states.pop(chat_id, None)
        tg_send(chat_id, "Login cancelled.", main_keyboard(chat_id))
        return
    provider = 'stexsms' if 'Stex' in text else 'mnitnetwork'
    with states_lock:
        user_states[chat_id] = {'step': 'awaiting_login_email', 'provider': provider}
    tg_send(chat_id, f"📧 <b>Enter your email for {text}:</b>\n\n<i>Example: user@example.com</i>", cancel_keyboard())

def process_login_email(chat_id, text, state):
    if text == '⬅️ Cancel':
        with states_lock:
            user_states.pop(chat_id, None)
        tg_send(chat_id, "Login cancelled.", main_keyboard(chat_id))
        return
    email = text.strip()
    if '@' not in email or '.' not in email:
        tg_send(chat_id, "❌ Invalid email format. Try again or Cancel.", cancel_keyboard())
        return
    state['email'] = email
    state['step'] = 'awaiting_login_password'
    tg_send(chat_id, "🔒 <b>Enter your password:</b>\n\n<i>Your password will be stored securely.</i>", cancel_keyboard())

def process_login_password(chat_id, text, state):
    if text == '⬅️ Cancel':
        with states_lock:
            user_states.pop(chat_id, None)
        tg_send(chat_id, "Login cancelled.", main_keyboard(chat_id))
        return
    password = text.strip()
    provider = state['provider']
    email = state['email']
    try:
        test_bot = StexSMS(provider=provider, email=email, password=password)
        test_bot.login()
    except Exception as e:
        tg_send(chat_id, f"❌ <b>Login failed:</b> {escape(str(e))}\n\nCheck your credentials.", cancel_keyboard())
        return
    save_credentials(chat_id, provider, email, password)
    with instances_lock:
        cache_key = (provider, chat_id)
        if cache_key in bot_instances:
            del bot_instances[cache_key]
    with states_lock:
        user_states.pop(chat_id, None)
    name = 'StexSMS' if provider == 'stexsms' else 'MNIT Network'
    tg_send(chat_id, f"✅ <b>Logged into {name}!</b>", main_keyboard(chat_id))

def handle_logout(chat_id):
    logout_user(chat_id)
    tg_send(chat_id, "🔓 <b>Logged out.</b> Using default accounts.", main_keyboard(chat_id))

# ---------- TempMail background (unchanged) ----------
def temp_inbox_watcher():
    while True:
        with temp_email_lock:
            snapshot = [(uid, list(info.get("emails", []))) for uid, info in user_temp_emails.items()]
        for uid, emails in snapshot:
            for entry in emails:
                email = entry["email"]
                last_id = entry.get("last_mail_id")
                try:
                    mail = fetch_latest_mail(email)
                except Exception:
                    continue
                if not mail:
                    continue
                mid = mail.get("mail_id")
                if mid == last_id:
                    continue
                body = fetch_mail_content(email, mid)
                subject = mail.get("subject", "") or ""
                otp = extract_otp_temp(body) or extract_otp_temp(subject)
                with temp_email_lock:
                    if uid not in user_temp_emails:
                        continue
                    for e in user_temp_emails[uid]["emails"]:
                        if e["email"] == email:
                            e["last_mail_id"] = mid
                            break
                text = (
                    f"📩 <b>New Email</b>\n\n"
                    f"📧 <b>To:</b> <code>{escape(email)}</code>\n"
                    f"📤 <b>From:</b> {escape(mail.get('from_mail',''))}\n"
                    f"📌 <b>Subject:</b> {escape(subject)}\n"
                )
                if otp:
                    text += f"\n🔑 <b>OTP:</b> <code>{otp}</code>\n"
                text += f"\n<pre>{escape(body)}</pre>"
                try:
                    requests.post(f"{TG_API}/sendMessage",
                                  data={'chat_id': uid, 'text': text, 'parse_mode': 'HTML'},
                                  timeout=5)
                except Exception:
                    pass
        time.sleep(FETCH_INTERVAL)

def temp_cleanup():
    while True:
        now = time.time()
        with temp_email_lock:
            to_del = [uid for uid, info in user_temp_emails.items()
                      if now - info.get("last_active", now) > INACTIVE_TIMEOUT]
            for uid in to_del:
                del user_temp_emails[uid]
        time.sleep(300)

threading.Thread(target=temp_inbox_watcher, daemon=True).start()
threading.Thread(target=temp_cleanup, daemon=True).start()

# ---------- Telegram polling with expanded callback handling ----------
def run_telegram_bot():
    warmup_default_bots()
    offset = 0
    while True:
        try:
            response = requests.get(f"{TG_API}/getUpdates",
                                    params={'offset': offset, 'timeout': 30},
                                    timeout=35)
            for update in response.json().get('result', []):
                offset = update['update_id'] + 1

                # ----- Callback queries (inline buttons) -----
                if 'callback_query' in update:
                    cq = update['callback_query']
                    chat_id = cq['message']['chat']['id']
                    data = cq['data']
                    try:
                        requests.post(f"{TG_API}/answerCallbackQuery",
                                      data={'callback_query_id': cq['id'], 'text': 'OK'},
                                      timeout=5)
                    except Exception:
                        pass

                    if data == 'profile_set_wallet':
                        tg_send(chat_id, "🔧 <b>Select wallet to set:</b>", wallet_method_keyboard())

                    elif data.startswith('wallet_'):
                        method = data.replace('wallet_', '')  # bkash, rocket, binance
                        with states_lock:
                            user_states[chat_id] = {'step': 'awaiting_wallet_detail', 'method': method}
                        if method == 'binance':
                            prompt = "🆔 <b>Enter your Binance UID:</b>"
                        else:
                            prompt = f"📱 <b>Enter your {method.capitalize()} number:</b>"
                        tg_send(chat_id, prompt, cancel_keyboard())

                    elif data == 'profile_withdraw':
                        tg_send(chat_id, "💸 <b>Select withdrawal method:</b>", withdraw_method_keyboard())

                    elif data.startswith('withdraw_method_'):
                        method = data.replace('withdraw_method_', '')
                        wallet = get_user_wallet(chat_id)
                        if method in ('bkash', 'rocket', 'binance'):
                            detail = wallet.get(method) or 'Not Set'
                            if detail == 'Not Set':
                                tg_send(chat_id, f"❌ Your {method} wallet is not set. Use 'Set wallet' first.", main_keyboard(chat_id))
                                continue
                        else:  # mobile recharge – use bkash
                            detail = wallet.get('bkash') or 'Not Set'
                            if detail == 'Not Set':
                                tg_send(chat_id, "❌ Please set your Bkash number first (used for mobile recharge).", main_keyboard(chat_id))
                                continue
                        with states_lock:
                            user_states[chat_id] = {
                                'step': 'awaiting_withdraw_amount',
                                'method': method,
                                'wallet_detail': detail
                            }
                        balance = get_user_balance(chat_id)
                        usd = balance / EXCHANGE_RATE
                        msg = (
                            f"💰 <b>Current Balance:</b> {balance:.2f} BDT / ${usd:.4f}\n"
                            f"💳 <b>Minimum Withdrawal:</b> 20 BDT / $0.50\n\n"
                            f"<b>Please enter the amount you want to withdraw (in BDT):</b>"
                        )
                        tg_send(chat_id, msg, cancel_keyboard())

                    elif data.startswith('admin_complete_'):
                        if not is_admin(chat_id):
                            tg_send(chat_id, "Unauthorized.")
                            continue
                        req_id = int(data.split('_')[-1])
                        completed_user = complete_withdrawal(req_id, chat_id)
                        if completed_user:
                            tg_send(chat_id, f"✅ Withdrawal request #{req_id} marked as completed and user notified.")
                        else:
                            tg_send(chat_id, "❌ Request not found or already processed.")
                        pendings = get_pending_requests()
                        if not pendings:
                            tg_send(chat_id, "No pending withdrawal requests.")
                        else:
                            lines = []
                            for p in pendings:
                                lines.append(
                                    f"🔹 <b>ID:</b> {p['id']} | <b>User:</b> {p['user_id']}\n"
                                    f"   💵 {p['amount_bdt']} BDT via {p['method']} ({p['wallet_detail']})\n"
                                    f"   🕒 {p['time']}"
                                )
                            kb = {
                                'inline_keyboard': [
                                    [{'text': f'✅ Complete #{p["id"]}', 'callback_data': f'admin_complete_{p["id"]}'}]
                                    for p in pendings
                                ]
                            }
                            tg_send(chat_id, "📋 <b>Pending Withdrawals:</b>\n\n" + "\n\n".join(lines), kb)

                    else:
                        if data == 'go_back':
                            tg_send(chat_id, 'Main menu:', main_keyboard(chat_id))
                        else:
                            pass
                    continue

                # ----- Text messages -----
                if 'message' in update:
                    text = update['message'].get('text', '').strip()
                    chat_id = update['message']['chat']['id']

                    with states_lock:
                        state = user_states.get(chat_id)

                    # state machine for wallet detail input
                    if state and state.get('step') == 'awaiting_wallet_detail':
                        if text == '⬅️ Cancel':
                            with states_lock:
                                user_states.pop(chat_id, None)
                            tg_send(chat_id, "Wallet setting cancelled.", main_keyboard(chat_id))
                            continue
                        method = state['method']
                        if method in ('bkash', 'rocket'):
                            if not re.fullmatch(r'\d{7,15}', text):
                                tg_send(chat_id, "❌ Invalid phone number. Must be digits, 7‑15 characters.", cancel_keyboard())
                                continue
                        elif method == 'binance':
                            if not re.fullmatch(r'\d{6,}', text):
                                tg_send(chat_id, "❌ Invalid Binance UID. Must be numeric.", cancel_keyboard())
                                continue
                        set_wallet_detail(chat_id, method, text)
                        with states_lock:
                            user_states.pop(chat_id, None)
                        bal_text, bal_kb = format_balance_message(chat_id)
                        tg_send(chat_id, bal_text, bal_kb)
                        continue

                    # state for withdraw amount
                    if state and state.get('step') == 'awaiting_withdraw_amount':
                        if text == '⬅️ Cancel':
                            with states_lock:
                                user_states.pop(chat_id, None)
                            tg_send(chat_id, "Withdrawal cancelled.", main_keyboard(chat_id))
                            continue
                        try:
                            amount = float(text)
                        except ValueError:
                            tg_send(chat_id, "❌ Invalid amount. Enter a number.", cancel_keyboard())
                            continue
                        if amount < 20:
                            tg_send(chat_id, "❌ Minimum withdrawal is 20 BDT.", cancel_keyboard())
                            continue
                        success, err = create_withdrawal(chat_id, amount, state['method'], state['wallet_detail'])
                        with states_lock:
                            user_states.pop(chat_id, None)
                        if success:
                            tg_send(chat_id, "✅ Your withdrawal request has been submitted and is processing. You will be notified upon completion.", main_keyboard(chat_id))
                        else:
                            tg_send(chat_id, f"❌ {err}", main_keyboard(chat_id))
                        continue

                    # Existing login flow
                    if state:
                        step = state.get('step')
                        if step == 'awaiting_login_provider':
                            process_login_provider(chat_id, text); continue
                        elif step == 'awaiting_login_email':
                            process_login_email(chat_id, text, state); continue
                        elif step == 'awaiting_login_password':
                            process_login_password(chat_id, text, state); continue
                        elif step == 'awaiting_range':
                            if text == '⬅️ Back':
                                with states_lock: user_states.pop(chat_id, None)
                                tg_send(chat_id, 'Select provider:', provider_keyboard()); continue
                            if not validate_range(text):
                                tg_send(chat_id, '❌ Invalid range! Must contain <b>XXX</b> and only digits/X.\nExample: <code>2250163333XXX</code>'); continue
                            prov = state['provider']
                            with states_lock: user_states.pop(chat_id, None)
                            tg_send(chat_id, f"🔍 Getting number from: <code>{escape(text)}</code>...")
                            handle_create_number(prov, chat_id, manual_range=text); continue
                        elif step == 'choose_range_mode':
                            prov = state['provider']
                            if text == '🎲 Random Range':
                                with states_lock: user_states.pop(chat_id, None)
                                handle_create_number(prov, chat_id); continue
                            elif text == '✏️ Manual Range':
                                with states_lock: user_states[chat_id] = {'step': 'awaiting_range', 'provider': prov}
                                prompt = '✏️ <b>Enter the range:</b>\n\n📝 Example: <code>2250163333XXX</code>\n📝 Example: <code>67077267XXX</code>\n\n⚠️ Must contain <b>XXX</b>'
                                latest = user_latest_range.get(chat_id)
                                if latest: prompt += f'\n\n📝 <b>Latest Range:</b> <code>{escape(latest)}</code>'
                                tg_send(chat_id, prompt, {'keyboard': [[{'text': '⬅️ Back'}]], 'resize_keyboard': True}); continue
                            elif text == '⬅️ Back':
                                with states_lock: user_states.pop(chat_id, None)
                                tg_send(chat_id, 'Select provider:', provider_keyboard()); continue
                        elif step == 'awaiting_gender':
                            if text == '⬅️ Back':
                                with states_lock: user_states.pop(chat_id, None)
                                tg_send(chat_id, 'Welcome!', main_keyboard(chat_id)); continue
                            elif text in ['👨 Male', '👩 Female']:
                                gender = 'male' if 'Male' in text else 'female'
                                with states_lock: user_states.pop(chat_id, None)
                                tg_send(chat_id, format_identity_message(gender), main_keyboard(chat_id)); continue
                        elif step == 'awaiting_2fa_secret':
                            if text == '⬅️ Back':
                                with states_lock: user_states.pop(chat_id, None)
                                tg_send(chat_id, 'Welcome!', main_keyboard(chat_id)); continue
                            else:
                                with states_lock: user_states.pop(chat_id, None)
                                msg, success = format_2fa_code(text)
                                tg_send(chat_id, msg, main_keyboard(chat_id)); continue
                        elif step == 'awaiting_temp_domain':
                            if text == '⬅️ Cancel':
                                with states_lock: user_states.pop(chat_id, None)
                                tg_send(chat_id, 'Cancelled.', main_keyboard(chat_id)); continue
                            if text not in AVAILABLE_DOMAINS:
                                tg_send(chat_id, 'Please select a domain.', temp_domain_keyboard()); continue
                            email = generate_temp_email(text)
                            with temp_email_lock:
                                if chat_id not in user_temp_emails:
                                    user_temp_emails[chat_id] = {"emails": [], "last_active": time.time()}
                                user_temp_emails[chat_id]["emails"].append({"email": email, "last_mail_id": None})
                                user_temp_emails[chat_id]["emails"] = user_temp_emails[chat_id]["emails"][-MAX_EMAILS:]
                                user_temp_emails[chat_id]["last_active"] = time.time()
                            with states_lock: user_states.pop(chat_id, None)
                            tg_send(chat_id, f"📧 <b>Your Temp Email</b>\n\n<code>{email}</code>\n\nInbox is monitored.", main_keyboard(chat_id)); continue

                    # ----- Main menu commands -----
                    if text.startswith('/start'):
                        parts = text.split()
                        payload = parts[1] if len(parts) > 1 else None
                        with states_lock: user_states.pop(chat_id, None)
                        if payload == 'getnumber':
                            tg_send(chat_id, 'Select provider:', provider_keyboard())
                        else:
                            tg_send(chat_id, 'Welcome! Choose an option:', main_keyboard(chat_id))
                        continue

                    if text == '⬅️ Back':
                        with states_lock: user_states.pop(chat_id, None)
                        tg_send(chat_id, 'Welcome! Choose an option:', main_keyboard(chat_id))

                    elif text == '📞 Get Number':
                        tg_send(chat_id, 'Select provider:', provider_keyboard())
                    elif text == '🔄 Change Number':
                        latest_range = user_latest_range.get(chat_id)
                        latest_provider = user_latest_provider.get(chat_id)
                        if latest_range and latest_provider:
                            tg_send(chat_id, f"🔄 Fetching new number from range: <code>{escape(latest_range)}</code>...")
                            handle_create_number(latest_provider, chat_id, manual_range=latest_range)
                        else:
                            tg_send(chat_id, "❌ No manual range found.\nUse <b>📞 Get Number</b> → <b>✏️ Manual Range</b> first.", main_keyboard(chat_id))

                    elif text == '🌐 StexSMS':
                        with states_lock: user_states[chat_id] = {'step': 'choose_range_mode', 'provider': 'stexsms'}
                        tg_send(chat_id, '🔧 <b>Choose range mode:</b>', range_mode_keyboard())
                    elif text == '🌐 MNIT Network':
                        with states_lock: user_states[chat_id] = {'step': 'choose_range_mode', 'provider': 'mnitnetwork'}
                        tg_send(chat_id, '🔧 <b>Choose range mode:</b>', range_mode_keyboard())
                    elif text == '👤 Fake Name':
                        with states_lock: user_states[chat_id] = {'step': 'awaiting_gender'}
                        tg_send(chat_id, '👤 <b>Select Gender:</b>', gender_keyboard())
                    elif text == '🔐 Get 2FA':
                        with states_lock: user_states[chat_id] = {'step': 'awaiting_2fa_secret'}
                        tg_send(chat_id, "📲 <b>Paste your 2FA Secret Key</b>\n\n<code>ABCD EFGH IJKL MNOP QRS2 TUV7</code>\n<i>(Copy the format)</i>",
                                {'keyboard': [[{'text': '⬅️ Back'}]], 'resize_keyboard': True})
                    elif text in ['🔐 Log IN', '🔓 Logout']:
                        has_creds = any(get_credentials(chat_id, p)[0] for p in ['stexsms', 'mnitnetwork'])
                        if has_creds:
                            handle_logout(chat_id)
                        else:
                            start_login(chat_id)
                    elif text == '📧 Temp Mail':
                        with states_lock: user_states[chat_id] = {'step': 'awaiting_temp_domain'}
                        tg_send(chat_id, '🌐 <b>Select a domain:</b>', temp_domain_keyboard())

                    # ----- NEW: My Profile menu -----
                    elif text == '👤 My Profile':
                        with states_lock: user_states.pop(chat_id, None)
                        tg_send(chat_id, "👤 <b>Profile Menu</b>", profile_keyboard(chat_id))
                    elif text == '💰 Balance':
                        bal_text, bal_kb = format_balance_message(chat_id)
                        tg_send(chat_id, bal_text, bal_kb)
                    elif text == '📋 Withdraw History':
                        history = get_withdrawal_history(chat_id)
                        if not history:
                            tg_send(chat_id, "📭 No completed withdrawals yet.")
                        else:
                            lines = []
                            for h in history:
                                lines.append(
                                    f"🔹 <b>ID:</b> {h['id']} | <b>Method:</b> {h['method']}\n"
                                    f"   💵 {h['amount_bdt']} BDT | 🕒 {h['completed_time']}"
                                )
                            tg_send(chat_id, "📋 <b>Your Withdrawal History:</b>\n\n" + "\n\n".join(lines))
                    elif text == '📋 Withdraw List':
                        if not is_admin(chat_id):
                            tg_send(chat_id, "❌ Unauthorized.")
                            continue
                        pendings = get_pending_requests()
                        if not pendings:
                            tg_send(chat_id, "No pending withdrawal requests.")
                        else:
                            lines = []
                            kb_buttons = []
                            for p in pendings:
                                lines.append(
                                    f"🔹 <b>ID:</b> {p['id']} | <b>User:</b> {p['user_id']}\n"
                                    f"   💵 {p['amount_bdt']} BDT via {p['method']} ({p['wallet_detail']})\n"
                                    f"   🕒 {p['time']}"
                                )
                                kb_buttons.append([{'text': f'✅ Complete #{p["id"]}', 'callback_data': f'admin_complete_{p["id"]}'}])
                            kb = {'inline_keyboard': kb_buttons}
                            tg_send(chat_id, "📋 <b>Pending Withdrawals:</b>\n\n" + "\n\n".join(lines), kb)

                    # unknown text → main menu fallback
                    else:
                        tg_send(chat_id, "I didn't understand that. Use the menu buttons.", main_keyboard(chat_id))

        except requests.exceptions.Timeout:
            continue
        except requests.exceptions.ConnectionError:
            time.sleep(2)
        except Exception:
            time.sleep(2)

if __name__ == '__main__':
    run_telegram_bot()
