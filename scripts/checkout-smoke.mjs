// 收银台主题冒烟测试：在最小 DOM 环境里真正执行主题脚本并走一遍启动流程。
//
// 仅做语法解析（new Function）无法发现运行时错误——曾因此漏掉 localeUrl 自递归，
// 导致收银台白屏卡在「正在读取订单」。这里会实际调用 Payment.init() 与语言切换。
//
// 用法：node scripts/checkout-smoke.mjs

import fs from 'node:fs';
import vm from 'node:vm';
import path from 'node:path';

const ROOT = path.resolve(import.meta.dirname, '..');
const VERSION = 'v9.9.9-smoke';

let failures = 0;
const fail = (msg) => { failures++; console.error('  ✗ ' + msg); };
const pass = (msg) => console.log('  ✓ ' + msg);

/** 极简元素桩：主题脚本只用到属性读写、class、事件与子节点操作 */
function makeElement(tag = 'div') {
  const el = {
    tagName: tag.toUpperCase(),
    style: {},
    dataset: {},
    children: [],
    attributes: {},
    textContent: '',
    innerHTML: '',
    value: '',
    href: '',
    hidden: false,
    classList: { add() {}, remove() {}, toggle() {}, contains: () => false },
    setAttribute(k, v) { this.attributes[k] = String(v); },
    getAttribute(k) { return Object.prototype.hasOwnProperty.call(this.attributes, k) ? this.attributes[k] : null; },
    removeAttribute(k) { delete this.attributes[k]; },
    hasAttribute(k) { return Object.prototype.hasOwnProperty.call(this.attributes, k); },
    appendChild(child) { this.children.push(child); return child; },
    removeChild() {},
    addEventListener() {},
    removeEventListener() {},
    querySelector() { return makeElement(); },
    querySelectorAll() { return []; },
    closest() { return null; },
    focus() {},
    click() {},
    insertAdjacentHTML() {},
    getBoundingClientRect: () => ({ width: 200, height: 200, top: 0, left: 0 }),
    clientWidth: 200,
    clientHeight: 200,
  };
  return el;
}

function makeSandbox(fetchImpl) {
  const elements = new Map();
  const document = {
    documentElement: makeElement('html'),
    body: makeElement('body'),
    title: '',
    getElementById(id) {
      if (!elements.has(id)) elements.set(id, makeElement());
      return elements.get(id);
    },
    querySelector() { return makeElement(); },
    querySelectorAll() { return []; },
    createElement: (tag) => makeElement(tag),
    createTextNode: () => makeElement('text'),
    addEventListener() {},
    removeEventListener() {},
  };

  const storage = new Map();
  const window = {
    document,
    location: { href: 'https://pay.example.com/pay/checkout/T1', search: '', protocol: 'https:', host: 'pay.example.com' },
    navigator: { language: 'zh-CN', languages: ['zh-CN', 'zh'], clipboard: { writeText: () => Promise.resolve() }, userAgent: 'node-smoke' },
    localStorage: {
      getItem: (k) => (storage.has(k) ? storage.get(k) : null),
      setItem: (k, v) => storage.set(k, String(v)),
      removeItem: (k) => storage.delete(k),
    },
    __CHECKOUT_VERSION__: VERSION,
    addEventListener() {},
    removeEventListener() {},
    setTimeout, clearTimeout, setInterval, clearInterval,
    fetch: fetchImpl,
    encodeURIComponent,
    matchMedia: () => ({ matches: false, addEventListener() {}, removeEventListener() {} }),
  };

  const sandbox = {
    window, document, console,
    navigator: window.navigator,
    localStorage: window.localStorage,
    location: window.location,
    fetch: fetchImpl,
    setTimeout, clearTimeout, setInterval, clearInterval,
    encodeURIComponent, decodeURIComponent, URL, URLSearchParams,
    module: {}, exports: {},
  };
  sandbox.self = window;
  sandbox.globalThis = sandbox;
  window.window = window;
  vm.createContext(sandbox);
  return sandbox;
}

/** 记录所有请求；语言文件返回桩翻译，订单接口返回一张待选币种的订单 */
function makeFetch(requests) {
  return (url, init) => {
    requests.push(String(url));
    const body = String(url).includes('/locales/')
      ? { payment: { title: 'T', networks: {} } }
      : { status_code: 1, message: 'ok', data: { trade_id: 'T1', order_id: 'M1', status: 1, money: '1.00', fiat: 'CNY', expired_at: Math.floor(Date.now() / 1000) + 600, created_at: Math.floor(Date.now() / 1000), methods: [] } };
    return Promise.resolve({
      ok: true, status: 200,
      json: () => Promise.resolve(body),
      text: () => Promise.resolve(JSON.stringify(body)),
    });
  };
}

function loadTheme(theme, requests) {
  const sandbox = makeSandbox(makeFetch(requests));
  const dir = path.join(ROOT, 'static/checkout', theme, 'assets/js');

  const i18nextPath = path.join(dir, 'i18next.min.js');
  if (fs.existsSync(i18nextPath)) {
    vm.runInContext(fs.readFileSync(i18nextPath, 'utf8'), sandbox, { filename: 'i18next.min.js' });
    const i18next = sandbox.i18next || sandbox.window.i18next || sandbox.module.exports;
    sandbox.i18next = i18next;
    sandbox.window.i18next = i18next;
  }

  vm.runInContext(fs.readFileSync(path.join(dir, 'checkout.js'), 'utf8'), sandbox, { filename: `${theme}/checkout.js` });
  return sandbox;
}

const tick = (ms = 60) => new Promise((r) => setTimeout(r, ms));

async function smokeSufe() {
  console.log('[sufe]');
  const requests = [];
  const sandbox = loadTheme('sufe', requests);
  const win = sandbox.window;

  if (typeof win.Payment?.init !== 'function') return fail('Payment.init 未暴露');
  if (typeof win.changeLanguage !== 'function') return fail('changeLanguage 未暴露');

  // 启动：必须真正发出语言文件与订单接口请求，而不是在此之前抛错
  try {
    win.Payment.init({ trade_id: 'T1' });
  } catch (err) {
    return fail('Payment.init 抛错：' + (err && err.message ? err.message : err));
  }
  await tick();

  const locale = requests.find((u) => u.includes('/locales/'));
  if (!locale) return fail('启动后没有请求语言文件（脚本可能在 init 阶段抛错）');
  pass('启动请求语言文件：' + locale);

  if (!locale.includes('?v=' + encodeURIComponent(VERSION))) fail('语言文件请求缺少版本号：' + locale);
  else pass('语言文件请求带版本号');

  if (!requests.some((u) => u.includes('/api/v1/pay/'))) fail('启动后没有请求支付接口');
  else pass('启动请求支付接口');

  // 语言切换：14 种都要真正发起请求（旧版脚本只放行 zh/en）
  const langs = ['zh-TW', 'en', 'ja', 'ko', 'ru', 'vi', 'th', 'id', 'es', 'pt', 'fr', 'de', 'tr'];
  for (const lang of langs) {
    const before = requests.length;
    try {
      win.changeLanguage(lang);
    } catch (err) {
      fail(`切换到 ${lang} 抛错：` + (err && err.message ? err.message : err));
      continue;
    }
    await tick(20);
    const hit = requests.slice(before).find((u) => u.includes(`/locales/${lang}.json`));
    if (!hit) fail(`切换到 ${lang} 没有请求对应语言文件`);
  }
  if (!failures) pass(`14 种语言均可切换`);
}

async function smokeOfficial() {
  console.log('[official]');
  const requests = [];
  const sandbox = loadTheme('official', requests);
  const win = sandbox.window;

  if (typeof win.Payment?.initI18n !== 'function') return fail('Payment.initI18n 未暴露');
  try {
    win.Payment.initI18n();
  } catch (err) {
    return fail('initI18n 抛错：' + (err && err.message ? err.message : err));
  }
  await tick();

  const locale = requests.find((u) => u.includes('/locales/'));
  if (!locale) return fail('没有请求语言文件');
  if (!locale.includes('?v=' + encodeURIComponent(VERSION))) fail('语言文件请求缺少版本号：' + locale);
  else pass('语言文件请求带版本号：' + locale);
}

async function smokeLangge() {
  console.log('[langge]');
  try {
    loadTheme('langge', []);   // 翻译内联，只验证脚本可加载且不抛错
  } catch (err) {
    return fail('脚本加载抛错：' + (err && err.message ? err.message : err));
  }
  pass('脚本加载正常');
}

await smokeSufe();
await smokeOfficial();
await smokeLangge();

if (failures) {
  console.error(`\n收银台冒烟测试失败：${failures} 项`);
  process.exit(1);
}
console.log('\n收银台冒烟测试通过');
