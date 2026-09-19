/*
 * 苏菲家宽 · 收银台脚本（BEpusdt checkout 模板格式）
 * 与 checkout.css 设计令牌完全对齐；所有订单数据均通过前端 AJAX 获取。
 *
 * 服务端只注入 {{ .trade_id }}，其余全部来自三个接口：
 *   POST /api/v1/pay/info          读取订单（含状态、金额、地址、到期时间）
 *   POST /api/v1/pay/methods       读取可选币种 / 网络
 *   POST /api/v1/pay/update-order  确认付款方式，返回 payment_url
 *
 * 单页两阶段：未确认付款方式 → 选择器；已确认 → 二维码 + 地址。
 */
(function () {
    'use strict';

    const ASSETS = '/checkout/sufe/assets';
    const WEB3 = ASSETS + '/web3icons';
    const LOCALES = ASSETS + '/locales';

    const POLL_INTERVAL = 5000;      // 状态轮询间隔
    const URGENT_SECONDS = 300;      // 倒计时进入 coral 紧急态
    const BLINK_SECONDS = 60;        // 倒计时开始闪烁
    const SEARCH_THRESHOLD = 6;      // 下拉项超过该数量才注入搜索框

    const STATUS_WAITING = 1;
    const STATUS_SUCCESS = 2;
    const STATUS_EXPIRED = 3;
    const STATUS_CANCELED = 4;
    const STATUS_CONFIRMING = 5;
    const STATUS_FAILED = 6;

    /* 各链原生资产 —— 用于「请勿误转 XXX」提示 */
    const NATIVE_ASSET = {
        tron: 'TRX',
        ethereum: 'ETH',
        bsc: 'BNB',
        polygon: 'POL',
        arbitrum: 'ETH',
        base: 'ETH',
        solana: 'SOL',
        aptos: 'APT',
        ton: 'TON',
        plasma: 'XPL',
        xlayer: 'OKB'
    };

    /* 原生币种（付的就是链本身的资产，不需要「勿误转」提示） */
    const NATIVE_CURRENCIES = ['TRX', 'BNB', 'ETH', 'GRAM', 'POL', 'SOL', 'APT', 'TON'];

    let i18nInitialized = false;
    let currentLang = 'zh';
    /* 支持的界面语言；navigator.language 归一化到其中之一，找不到退回 en */
    const SUPPORTED_LANGS = ['zh', 'zh-TW', 'en', 'ja', 'ko', 'ru', 'vi', 'th', 'id', 'es', 'pt', 'fr', 'de', 'tr'];
    const HTML_LANG = { zh: 'zh-CN', 'zh-TW': 'zh-TW', pt: 'pt-BR' };

    function normalizeLang(raw) {
        const value = String(raw || '').trim();
        if (!value) return '';
        if (SUPPORTED_LANGS.indexOf(value) !== -1) return value;
        const lowerValue = value.toLowerCase();
        if (lowerValue === 'zh-tw' || lowerValue === 'zh-hk' || lowerValue === 'zh-mo' || lowerValue === 'zh-hant') return 'zh-TW';
        const base = lowerValue.split('-')[0];
        return SUPPORTED_LANGS.indexOf(base) !== -1 ? base : '';
    }

    let tradeId = '';
    let orderData = null;
    let paymentMethods = [];
    let networkSort = '';
    let selectedCurrency = '';
    let selectedMethod = null;

    let stage = 'loading';           // loading | selection | payment
    let settled = false;             // 已进入终态（成功 / 取消 / 超时 / 失败）
    let countdownTimer = null;
    let statusTimer = null;
    let toastTimer = null;
    let qrRenderedSize = 0;
    let qrRenderedText = '';

    const dom = {};

    /* ============================================================
       工具
       ============================================================ */

    function $id(id) {
        return document.getElementById(id);
    }

    function show(el) {
        if (el) el.removeAttribute('hidden');
    }

    function hide(el) {
        if (el) el.setAttribute('hidden', 'hidden');
    }

    function apiPost(path, body) {
        return fetch(path, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(body || {})
        }).then(function (r) { return r.json(); });
    }

    function pad2(n) {
        return n < 10 ? '0' + n : String(n);
    }

    function upper(v) {
        return (v == null ? '' : String(v)).toUpperCase();
    }

    function lower(v) {
        return (v == null ? '' : String(v)).toLowerCase();
    }

    function escapeHtml(text) {
        return String(text == null ? '' : text)
            .replace(/&/g, '&amp;')
            .replace(/</g, '&lt;')
            .replace(/>/g, '&gt;')
            .replace(/"/g, '&quot;');
    }

    /* 只放行 http(s) 与站内相对地址，挡掉 javascript: 之类的协议 */
    function safeUrl(url) {
        const value = String(url == null ? '' : url).trim();
        if (!value) return '';
        if (/^https?:\/\//i.test(value)) return value;
        if (value.charAt(0) === '/' && value.charAt(1) !== '/') return value;
        return '';
    }

    function tokenIcon(currency) {
        return WEB3 + '/token/' + upper(currency) + '.svg';
    }

    function networkIcon(network) {
        return WEB3 + '/network/' + lower(network) + '.svg';
    }

    /* 图标可能缺失（新增链尚未补 svg）—— 加载失败就隐藏，不留破图 */
    function bindIconFallback(img) {
        if (!img) return;
        img.addEventListener('error', function () {
            hide(img);
        });
    }

    /* ============================================================
       i18n
       ============================================================ */

    function detectLang() {
        let lang = '';
        try {
            lang = localStorage.getItem('payment_language') || '';
        } catch (e) {
            lang = '';
        }
        lang = normalizeLang(lang);
        if (!lang) {
            const candidates = (navigator.languages && navigator.languages.length) ? navigator.languages : [navigator.language || navigator.userLanguage || 'en'];
            for (let i = 0; i < candidates.length && !lang; i++) lang = normalizeLang(candidates[i]);
        }
        return lang || 'en';
    }

    function initI18n() {
        currentLang = detectLang();

        return new Promise(function (resolve) {
            if (typeof i18next === 'undefined') {
                console.error('i18next is not loaded');
                resolve();
                return;
            }

            i18next.init({ lng: currentLang, fallbackLng: 'en', debug: false, resources: {} }, function (err) {
                if (err) {
                    console.error('i18next initialization failed:', err);
                    resolve();
                    return;
                }

                fetch(LOCALES + '/' + currentLang + '.json')
                    .then(function (r) { return r.json(); })
                    .then(function (translations) {
                        i18next.addResourceBundle(currentLang, 'translation', translations);
                        i18nInitialized = true;
                        if (dom.languageSwitcher) dom.languageSwitcher.value = currentLang;
                        applyI18n();
                        resolve();
                    })
                    .catch(function (error) {
                        console.error('Failed to load language file:', error);
                        resolve();
                    });
            });
        });
    }

    function changeLanguage(lang) {
        if (typeof i18next === 'undefined') return;
        lang = normalizeLang(lang);
        if (!lang) return;

        const done = function () {
            currentLang = lang;
            try {
                localStorage.setItem('payment_language', lang);
            } catch (e) { /* 隐私模式下 localStorage 不可写，忽略 */ }
            applyI18n();
            rebuildSelectors();
        };

        if (i18next.hasResourceBundle(lang, 'translation')) {
            i18next.changeLanguage(lang, function (err) {
                if (err) {
                    console.error('Language change failed:', err);
                    return;
                }
                done();
            });
            return;
        }

        fetch(LOCALES + '/' + lang + '.json')
            .then(function (r) { return r.json(); })
            .then(function (translations) {
                i18next.addResourceBundle(lang, 'translation', translations);
                return i18next.changeLanguage(lang);
            })
            .then(done)
            .catch(function (error) {
                console.error('Failed to load language file:', error);
            });
    }

    /* 当前上下文（已确认订单 / 已选付款方式）对应的展示字段。
       阶段二优先用订单本身；阶段一（含「返回重选」后）只看用户当前的选择。 */
    function context() {
        if (stage === 'payment' && orderData && orderData.token) {
            const net = orderData.network || {};
            const alias = String(net.alias || '');
            const parts = alias.split('・');
            return {
                token: upper(net.crypto || ''),
                networkId: lower(net.network || ''),
                networkLabel: upper(parts.length > 1 ? parts[parts.length - 1] : (net.name || net.network || ''))
            };
        }

        if (selectedMethod) {
            return {
                token: upper(selectedMethod.currency),
                networkId: lower(selectedMethod.network),
                networkLabel: upper(selectedMethod.token_net_name || selectedMethod.network)
            };
        }

        if (selectedCurrency) {
            return { token: upper(selectedCurrency), networkId: '', networkLabel: '' };
        }

        return { token: '', networkId: '', networkLabel: '' };
    }

    /* 链的本地化友好名：locales 里的 payment.networks.<id>，缺失时退回接口值 */
    function networkDisplayName(networkId, fallback) {
        if (!networkId) return fallback || '';
        if (i18nInitialized && typeof i18next !== 'undefined') {
            const key = 'payment.networks.' + networkId;
            const value = i18next.t(key);
            if (value && value !== key) return value;
        }
        return fallback || networkId;
    }

    function replacePlaceholders(text) {
        if (text == null) return '';

        const ctx = context();
        const token = ctx.token || '--';
        const network = ctx.networkLabel || '--';
        const networkName = networkDisplayName(ctx.networkId, ctx.networkLabel) || '--';
        const warningToken = NATIVE_ASSET[ctx.networkId] || '';
        const amountValue = currentAmountText(ctx.token);

        return String(text)
            .replace(/\{\{token\}\}/g, token)
            .replace(/\{\{network\}\}/g, network)
            .replace(/\{\{networkName\}\}/g, networkName)
            .replace(/\{\{warningToken\}\}/g, warningToken)
            .replace(/\{\{amount\}\}/g, amountValue);
    }

    /* 当前应到账数额（含币种），用于“手续费自理、必须足额到账”的提示 */
    function currentAmountText(token) {
        let amount = '';
        if (stage === 'payment' && orderData && orderData.actual_amount) amount = String(orderData.actual_amount);
        else if (selectedMethod && selectedMethod.actual_amount) amount = String(selectedMethod.actual_amount);
        if (!amount) return '--';
        return token ? amount + ' ' + token : amount;
    }

    /* 交易所内部转账：没有链上地址，页面展示账户 UID 与转账指引 */
    function isExchangeOrder(data) {
        if (!data) return false;
        if (data.exchange) return true;
        if (data.network && data.network.exchange) return true;
        return !!(selectedMethod && selectedMethod.exchange);
    }

    function t(key, defaultValue) {
        if (i18nInitialized && typeof i18next !== 'undefined') {
            const value = i18next.t(key);
            if (value && value !== key) return replacePlaceholders(value);
        }
        return replacePlaceholders(defaultValue != null ? defaultValue : key);
    }

    function applyI18n() {
        if (!i18nInitialized || typeof i18next === 'undefined') return;

        document.querySelectorAll('[data-i18n]').forEach(function (element) {
            const key = element.getAttribute('data-i18n');
            if (!key) return;

            if (key.charAt(0) === '[') {
                const matches = key.match(/\[(.+?)\](.+)/);
                if (!matches) return;

                const attr = matches[1];
                const translation = replacePlaceholders(i18next.t(matches[2]));
                if (attr === 'html') {
                    element.innerHTML = translation;
                } else {
                    element.setAttribute(attr, translation);
                }
                return;
            }

            const translation = replacePlaceholders(i18next.t(key));
            if (element.tagName === 'INPUT' || element.tagName === 'TEXTAREA') {
                element.placeholder = translation;
            } else {
                element.innerHTML = translation;
            }
        });

        try {
            document.title = t('payment.pageTitle', '苏菲家宽 · 收银台');
            document.documentElement.lang = HTML_LANG[currentLang] || currentLang;
        } catch (e) { /* noop */ }
    }

    /* ============================================================
       toast / 复制
       ============================================================ */

    function showToast(message, type) {
        const el = dom.toast;
        if (!el || !message) return;

        el.textContent = message;
        el.className = 'sufe-toast is-visible' + (type ? ' is-' + type : '');
        clearTimeout(toastTimer);
        toastTimer = setTimeout(function () {
            el.className = 'sufe-toast' + (type ? ' is-' + type : '');
        }, 2600);
    }

    function writeClipboard(text) {
        if (navigator.clipboard && navigator.clipboard.writeText) {
            return navigator.clipboard.writeText(text);
        }
        return Promise.reject(new Error('clipboard unavailable'));
    }

    function legacyCopy(text) {
        const area = document.createElement('textarea');
        area.value = text;
        area.setAttribute('readonly', 'readonly');
        area.style.cssText = 'position:fixed;top:0;left:0;opacity:0;';
        document.body.appendChild(area);
        area.select();

        let ok = false;
        try {
            ok = document.execCommand('copy');
        } catch (e) {
            ok = false;
        }
        document.body.removeChild(area);

        return ok;
    }

    function copyText(text, onSuccess) {
        if (!text) return;

        const succeed = function () {
            if (onSuccess) onSuccess();
        };
        const fail = function () {
            showToast(t('payment.copyFailed', '复制失败，请手动复制'), 'error');
        };

        writeClipboard(text).then(succeed).catch(function () {
            if (legacyCopy(text)) succeed();
            else fail();
        });
    }

    /* 金额始终保留显示，复制结果通过 toast 提示。 */
    function copyAmount() {
        const amount = currentAmount();
        if (!amount) return;

        copyText(amount, function () {
            showToast(t('payment.amountCopied', '金额已复制'), 'success');
        });
    }

    function copyAddress() {
        const address = orderData ? (orderData.token || '') : '';
        if (!address) return;

        const button = dom.copyAddressBtn;
        copyText(address, function () {
            showToast(t('payment.addressCopied', '地址已复制'), 'success');
            if (!button) return;

            const original = button.textContent;
            button.textContent = t('payment.copied', '已复制');
            button.classList.add('is-copied');
            clearTimeout(button._restore);
            button._restore = setTimeout(function () {
                button.textContent = original;
                button.classList.remove('is-copied');
            }, 1800);
        });
    }

    function currentAmount() {
        if (selectedMethod && selectedMethod.actual_amount) return selectedMethod.actual_amount;
        if (orderData && orderData.actual_amount && stage === 'payment') return orderData.actual_amount;
        return '';
    }

    /* ============================================================
       下拉选择器
       ============================================================ */

    function selectText(selectEl) {
        return selectEl ? selectEl.querySelector('.coin-select-text') : null;
    }

    function resetTrigger(selectEl, i18nKey, fallback) {
        const textEl = selectText(selectEl);
        if (!textEl) return;

        const icon = selectEl.querySelector('.coin-select-icon');
        if (icon) icon.remove();

        textEl.setAttribute('data-i18n', i18nKey);
        textEl.classList.add('is-placeholder');
        textEl.textContent = t(i18nKey, fallback);

        const list = selectEl.querySelector('.options-list');
        if (list) {
            list.querySelectorAll('.option-item').forEach(function (item) {
                item.classList.remove('selected');
            });
        }
    }

    function fillTrigger(selectEl, item) {
        const textEl = selectText(selectEl);
        if (!textEl) return;

        const old = selectEl.querySelector('.coin-select-icon');
        if (old) old.remove();

        if (item.icon) {
            const img = document.createElement('img');
            img.className = 'coin-select-icon';
            img.src = item.icon;
            img.alt = '';
            bindIconFallback(img);
            selectEl.insertBefore(img, selectEl.firstChild);
        }

        textEl.removeAttribute('data-i18n');
        textEl.classList.remove('is-placeholder');
        textEl.textContent = item.label;
    }

    function setSelectEnabled(selectEl, enabled) {
        if (!selectEl) return;
        selectEl.classList.toggle('is-disabled', !enabled);
        selectEl.setAttribute('aria-disabled', enabled ? 'false' : 'true');
        if (!enabled) selectEl.classList.remove('active');
    }

    function closeAllDropdowns(except) {
        document.querySelectorAll('.coin-select').forEach(function (el) {
            if (el !== except) el.classList.remove('active');
        });
    }

    const CHECK_SVG = '<svg class="option-check" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none"' +
        ' stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">' +
        '<polyline points="20 6 9 17 4 12"/></svg>';

    const SEARCH_SVG = '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor"' +
        ' stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">' +
        '<circle cx="11" cy="11" r="8"/><line x1="21" y1="21" x2="16.65" y2="16.65"/></svg>';

    function renderOptions(selectEl, items, onSelect) {
        if (!selectEl) return;

        let list = selectEl.querySelector('.options-list');
        if (!list) {
            list = document.createElement('div');
            list.className = 'options-list';
            list.setAttribute('role', 'listbox');
            list.addEventListener('click', function (e) { e.stopPropagation(); });
            selectEl.appendChild(list);
        }

        list.innerHTML = '';

        let searchInput = null;
        if (items.length > SEARCH_THRESHOLD) {
            const wrap = document.createElement('div');
            wrap.className = 'options-search-wrap';
            wrap.innerHTML = SEARCH_SVG +
                '<input class="options-search" type="text" autocomplete="off" spellcheck="false"' +
                ' placeholder="' + escapeHtml(t('payment.searchPlaceholder', '搜索')) + '">';
            list.appendChild(wrap);
            searchInput = wrap.querySelector('.options-search');
        }

        const itemsBox = document.createElement('div');
        itemsBox.className = 'options-items';
        list.appendChild(itemsBox);

        function paint(filter) {
            itemsBox.innerHTML = '';

            const keyword = (filter || '').trim().toLowerCase();
            const visible = keyword
                ? items.filter(function (it) { return it.label.toLowerCase().indexOf(keyword) !== -1; })
                : items;

            if (!visible.length) {
                const empty = document.createElement('div');
                empty.className = 'options-empty';
                empty.textContent = t('payment.noMatch', '没有匹配项');
                itemsBox.appendChild(empty);
                return;
            }

            visible.forEach(function (item) {
                const el = document.createElement('div');
                el.className = 'option-item';
                el.setAttribute('role', 'option');
                el.setAttribute('data-value', item.value);

                let html = '';
                if (item.icon) {
                    html += '<img class="option-icon" src="' + escapeHtml(item.icon) + '" alt=""' +
                        ' onerror="this.style.visibility=\'hidden\'">';
                }
                html += '<span class="option-text">' + escapeHtml(item.label) + '</span>';
                if (item.badge) {
                    html += '<span class="option-badge' + (item.badgeType ? ' is-' + item.badgeType : '') + '">' +
                        escapeHtml(item.badge) + '</span>';
                }
                html += CHECK_SVG;
                el.innerHTML = html;

                if (item.selected) el.classList.add('selected');

                el.addEventListener('click', function (e) {
                    e.stopPropagation();
                    selectEl.classList.remove('active');
                    itemsBox.querySelectorAll('.option-item').forEach(function (other) {
                        other.classList.remove('selected');
                    });
                    el.classList.add('selected');
                    fillTrigger(selectEl, item);
                    if (onSelect) onSelect(item.value, item);
                });

                itemsBox.appendChild(el);
            });
        }

        paint('');

        if (searchInput) {
            searchInput.addEventListener('input', function () { paint(this.value); });
            searchInput.addEventListener('click', function (e) { e.stopPropagation(); });
            searchInput.addEventListener('keydown', function (e) { e.stopPropagation(); });
        }

        selectEl._paint = paint;
        selectEl._search = searchInput;
    }

    function bindSelectTrigger(selectEl) {
        if (!selectEl || selectEl.dataset.bound) return;
        selectEl.dataset.bound = '1';

        const toggle = function () {
            if (selectEl.classList.contains('is-disabled')) return;
            if (!selectEl.querySelector('.option-item') && !selectEl.querySelector('.options-empty')) return;

            const willOpen = !selectEl.classList.contains('active');
            closeAllDropdowns();
            selectEl.classList.toggle('active', willOpen);

            if (willOpen && selectEl._search) {
                selectEl._search.value = '';
                if (selectEl._paint) selectEl._paint('');
                setTimeout(function () { selectEl._search.focus(); }, 60);
            }
        };

        selectEl.addEventListener('click', function (e) {
            e.stopPropagation();
            toggle();
        });

        selectEl.addEventListener('keydown', function (e) {
            if (e.key === 'Enter' || e.key === ' ' || e.key === 'Spacebar') {
                e.preventDefault();
                toggle();
                return;
            }
            if (e.key === 'Escape') {
                selectEl.classList.remove('active');
            }
        });
    }

    /* 语言切换后重新渲染下拉，让选项文案（热门 / 搜索 / 空态）跟着变 */
    function rebuildSelectors() {
        if (stage !== 'selection') return;

        buildCurrencyOptions();
        if (selectedCurrency) {
            buildNetworkOptions();
        } else {
            resetTrigger(dom.networkSelect, 'payment.selectNetwork', '请选择网络');
        }
    }

    function buildCurrencyOptions() {
        const currencies = [];
        paymentMethods.forEach(function (m) {
            if (currencies.indexOf(m.currency) === -1) currencies.push(m.currency);
        });

        renderOptions(dom.currencySelect, currencies.map(function (c) {
            return {
                value: c,
                label: upper(c),
                icon: tokenIcon(c),
                selected: c === selectedCurrency
            };
        }), function (value, item) {
            selectedCurrency = value;
            selectedMethod = null;
            resetTrigger(dom.networkSelect, 'payment.selectNetwork', '请选择网络');
            buildNetworkOptions();
            setSelectEnabled(dom.networkSelect, true);
            refreshSelectionView();
            void item;
        });

        if (selectedCurrency) {
            fillTrigger(dom.currencySelect, {
                label: upper(selectedCurrency),
                icon: tokenIcon(selectedCurrency)
            });
        } else {
            resetTrigger(dom.currencySelect, 'payment.selectCurrency', '请选择币种');
        }
    }

    function buildNetworkOptions() {
        const list = sortByConfiguredNetwork(paymentMethods.filter(function (m) {
            return m.currency === selectedCurrency;
        }), networkSort);

        renderOptions(dom.networkSelect, list.map(function (m) {
            const label = upper(m.token_custom_name || m.token_net_name || m.network);
            return {
                value: m.token_net_name || m.network,
                label: label,
                icon: networkIcon(m.network),
                badge: m.is_popular ? t('payment.hotBadge', '热门') : '',
                badgeType: m.is_popular ? 'popular' : '',
                method: m,
                selected: !!(selectedMethod && selectedMethod === m)
            };
        }), function (value, item) {
            selectedMethod = item.method;
            refreshSelectionView();
            void value;
        });

        if (selectedMethod) {
            fillTrigger(dom.networkSelect, {
                label: upper(selectedMethod.token_custom_name || selectedMethod.token_net_name),
                icon: networkIcon(selectedMethod.network)
            });
        }
    }

    /* 后台「网络排序」配置：逗号分隔的 network id，命中的排前面 */
    function sortByConfiguredNetwork(list, sortConfig) {
        if (!Array.isArray(list) || typeof sortConfig !== 'string' || !sortConfig.trim()) return list;

        const rank = {};
        let cursor = 0;
        sortConfig.split(',').forEach(function (item) {
            const key = item.trim();
            if (!key || Object.prototype.hasOwnProperty.call(rank, key)) return;
            rank[key] = cursor++;
        });
        if (!cursor) return list;

        return list.map(function (method, index) {
            const network = method && typeof method.network === 'string' ? method.network.trim() : '';
            const configured = Object.prototype.hasOwnProperty.call(rank, network);
            return { method: method, index: index, configured: configured, rank: configured ? rank[network] : -1 };
        }).sort(function (a, b) {
            if (a.configured !== b.configured) return a.configured ? -1 : 1;
            if (a.configured && a.rank !== b.rank) return a.rank - b.rank;
            return a.index - b.index;
        }).map(function (entry) { return entry.method; });
    }

    /* ============================================================
       视图渲染
       ============================================================ */

    function setHeroIcon(currency) {
        if (!dom.heroTokenLogo || !dom.heroIconText) return;

        if (!currency) {
            hide(dom.heroTokenLogo);
            dom.heroTokenLogo.removeAttribute('src');
            show(dom.heroIconText);
            dom.heroIconText.textContent = '--';
            return;
        }

        dom.heroIconText.textContent = upper(currency);
        dom.heroTokenLogo.onload = function () {
            show(dom.heroTokenLogo);
            hide(dom.heroIconText);
        };
        dom.heroTokenLogo.onerror = function () {
            hide(dom.heroTokenLogo);
            show(dom.heroIconText);
        };
        dom.heroTokenLogo.src = tokenIcon(currency);
    }

    function setAmount(amount, currency) {
        if (!dom.amountNum) return;

        if (!amount) {
            dom.amountNum.textContent = '---';
            if (dom.payAmount) dom.payAmount.classList.add('is-empty');
            return;
        }

        dom.amountNum.textContent = amount + (currency ? ' ' + upper(currency) : '');
        if (dom.payAmount) dom.payAmount.classList.remove('is-empty');
    }

    function renderOrderBase(data) {
        if (dom.orderId) dom.orderId.textContent = data.order_id || '--';
        if (dom.orderMoney) dom.orderMoney.textContent = (data.money || '--') + ' ' + (data.fiat || '');

        if (data.name) {
            if (dom.orderName) dom.orderName.textContent = data.name;
            show(dom.orderNameRow);
        } else {
            hide(dom.orderNameRow);
        }

        bindSupport(data.support_url);
    }

    function bindSupport(rawUrl) {
        const el = dom.supportBtn;
        if (!el) return;

        const url = safeUrl(rawUrl);
        if (url) {
            el.href = url;
            el.removeAttribute('aria-disabled');
            show(el);
            return;
        }

        // 未配置客服链接时整个按钮隐藏，避免页面上出现一个点了没反应的问号
        el.href = '#';
        el.setAttribute('aria-disabled', 'true');
        hide(el);
    }

    /* 阶段一：选择币种 / 网络 */
    function showSelectionStage() {
        stage = 'selection';
        selectedMethod = null;

        hide(dom.skeleton);
        hide(dom.qrSection);
        show(dom.selectorSection);
        hide(dom.instrPayment);
        show(dom.instrSelection);

        setSelectEnabled(dom.currencySelect, true);
        setSelectEnabled(dom.networkSelect, !!selectedCurrency);
        refreshSelectionView();

        loadMethods();
    }

    function refreshSelectionView() {
        setHeroIcon(selectedMethod ? selectedMethod.currency : selectedCurrency);
        setAmount(selectedMethod ? selectedMethod.actual_amount : '', selectedMethod ? selectedMethod.currency : '');

        if (dom.payTitle) {
            dom.payTitle.setAttribute('data-i18n', selectedCurrency ? 'payment.title' : 'payment.titlePending');
        }
        if (dom.paySubtitle) {
            dom.paySubtitle.setAttribute('data-i18n', selectedMethod ? 'payment.subtitleSelected' : 'payment.subtitleSelect');
        }

        if (dom.netBadge) {
            if (selectedMethod) {
                dom.netBadge.setAttribute('data-i18n', 'payment.networkBadge');
                show(dom.netBadge);
            } else {
                dom.netBadge.removeAttribute('data-i18n');
                dom.netBadge.textContent = '';
                hide(dom.netBadge);
            }
        }

        if (dom.payBtn) dom.payBtn.disabled = !selectedMethod;

        applyI18n();
    }

    /* 阶段二：二维码 + 收款地址 */
    function showPaymentStage(data) {
        stage = 'payment';

        hide(dom.skeleton);
        hide(dom.selectorSection);
        show(dom.qrSection);
        hide(dom.instrSelection);
        show(dom.instrPayment);

        const ctx = context();
        setHeroIcon(ctx.token);
        setAmount(data.actual_amount, ctx.token);

        const exchange = isExchangeOrder(data);

        if (dom.payTitle) dom.payTitle.setAttribute('data-i18n', 'payment.title');
        if (dom.paySubtitle) dom.paySubtitle.setAttribute('data-i18n', exchange ? 'payment.subtitleExchange' : 'payment.subtitlePay');
        if (dom.netBadge) {
            dom.netBadge.setAttribute('data-i18n', 'payment.networkBadge');
            show(dom.netBadge);
        }
        if (dom.addressLabel) dom.addressLabel.setAttribute('data-i18n', exchange ? 'payment.accountLabel' : 'payment.addressLabel');

        const instrFirst = dom.instrPayment ? dom.instrPayment.querySelector('li') : null;
        if (instrFirst) {
            if (exchange) {
                const guideKey = ctx.networkId === 'binance' ? 'payment.instructionExchangeBinance'
                    : ctx.networkId === 'okx' ? 'payment.instructionExchangeOkx' : 'payment.instructionExchange';
                instrFirst.setAttribute('data-i18n', '[html]' + guideKey);
            } else {
                const isNative = NATIVE_CURRENCIES.indexOf(ctx.token) !== -1;
                const hasWarning = !!NATIVE_ASSET[ctx.networkId];
                instrFirst.setAttribute('data-i18n',
                    (isNative || !hasWarning) ? '[html]payment.instruction1Native' : '[html]payment.instruction1');
            }
        }

        renderAddress(data.token || '');
        if (exchange) {
            // 交易所账户没有可扫的地址二维码
            if (dom.qrCodeBox) hide(dom.qrCodeBox);
            hide(dom.qrLogoBadge);
        } else {
            if (dom.qrCodeBox) show(dom.qrCodeBox);
            renderQrLogo(ctx.token, ctx.networkId);
            renderQr(data.token || '');
        }

        if (dom.reselectBtn) {
            if (data.reselect) show(dom.reselectBtn);
            else hide(dom.reselectBtn);
        }

        applyI18n();
    }

    /* 地址首 4 / 尾 6 用 coral 强调，方便肉眼核对 */
    function renderAddress(address) {
        const el = dom.walletAddress;
        if (!el) return;

        el.textContent = '';
        if (!address) {
            el.textContent = '--';
            return;
        }

        const append = function (text, emphasized) {
            const span = document.createElement('span');
            span.textContent = text;
            if (emphasized) span.className = 'address-emphasis';
            el.appendChild(span);
        };

        if (address.length <= 10) {
            append(address, true);
            return;
        }

        append(address.slice(0, 4), true);
        append(address.slice(4, -6), false);
        append(address.slice(-6), true);
    }

    function renderQrLogo(currency, networkId) {
        if (!dom.qrLogoBadge || !dom.qrTokenLogo) return;

        hide(dom.qrLogoBadge);
        hide(dom.qrNetworkLogo);
        dom.qrTokenLogo.removeAttribute('src');
        if (dom.qrNetworkLogo) dom.qrNetworkLogo.removeAttribute('src');

        if (!currency) return;

        dom.qrTokenLogo.onload = function () { show(dom.qrLogoBadge); };
        dom.qrTokenLogo.onerror = function () { hide(dom.qrLogoBadge); };
        dom.qrTokenLogo.src = tokenIcon(currency);

        if (!networkId || !dom.qrNetworkLogo) return;
        dom.qrNetworkLogo.onload = function () { show(dom.qrNetworkLogo); };
        dom.qrNetworkLogo.onerror = function () { hide(dom.qrNetworkLogo); };
        dom.qrNetworkLogo.src = networkIcon(networkId);
    }

    /* 二维码按容器实际像素 1:1 绘制，缩放不失真；暖黑 / 米白配色（对比度 ≈ 16:1） */
    function renderQr(text) {
        const holder = dom.qrcode;
        if (!holder || !text) return;
        if (typeof window.jQuery === 'undefined' || !window.jQuery.fn.qrcode) {
            console.error('jquery.qrcode is not loaded');
            return;
        }

        const box = Math.min(holder.clientWidth || 0, holder.clientHeight || 0);
        const size = box > 80 ? Math.floor(box) : 160;

        if (qrRenderedText === text && Math.abs(qrRenderedSize - size) <= 4) return;

        qrRenderedText = text;
        qrRenderedSize = size;

        window.jQuery(holder).empty().qrcode({
            render: 'canvas',
            text: text,
            width: size,
            height: size,
            correctLevel: 2,             // H —— 30% 冗余，中心徽标不影响识别
            foreground: '#181715',
            background: '#faf9f5'
        });
    }

    /* ============================================================
       倒计时
       ============================================================ */

    function startCountdown(expiredAt, createdAt) {
        stopCountdown();

        const expire = parseInt(expiredAt, 10) || 0;
        const created = parseInt(createdAt, 10) || 0;
        if (!expire) return;

        const total = Math.max(1, expire - created);

        const tick = function () {
            const remaining = Math.max(0, expire - Math.floor(Date.now() / 1000));
            paintCountdown(remaining, total);

            if (remaining <= 0) {
                stopCountdown();
                if (!settled) {
                    settled = true;
                    stopStatusPolling();
                    showTimeoutModal();
                }
            }
        };

        tick();
        countdownTimer = setInterval(tick, 1000);
    }

    function paintCountdown(remaining, total) {
        const hours = Math.floor(remaining / 3600);
        const minutes = Math.floor((remaining % 3600) / 60);
        const seconds = remaining % 60;

        const showHours = total >= 3600 || hours > 0;
        if (showHours) {
            show(dom.hourUnit);
            show(dom.hourSeparator);
            if (dom.hours) dom.hours.textContent = pad2(hours);
        } else {
            hide(dom.hourUnit);
            hide(dom.hourSeparator);
        }

        if (dom.minutes) dom.minutes.textContent = pad2(showHours ? minutes : Math.floor(remaining / 60));
        if (dom.seconds) dom.seconds.textContent = pad2(seconds);

        const banner = dom.countdownBanner;
        if (!banner) return;

        banner.classList.toggle('is-urgent', remaining <= URGENT_SECONDS);
        banner.style.animation = (remaining > 0 && remaining <= BLINK_SECONDS)
            ? 'urgentBlink 1.4s ease-in-out infinite'
            : '';
    }

    function stopCountdown() {
        if (countdownTimer) {
            clearInterval(countdownTimer);
            countdownTimer = null;
        }
    }

    /* ============================================================
       状态轮询
       ============================================================ */

    function startStatusPolling() {
        stopStatusPolling();
        statusTimer = setInterval(checkStatus, POLL_INTERVAL);
    }

    function stopStatusPolling() {
        if (statusTimer) {
            clearInterval(statusTimer);
            statusTimer = null;
        }
    }

    function checkStatus() {
        if (!tradeId || settled) return;

        apiPost('/api/v1/pay/info', { trade_id: tradeId })
            .then(function (res) {
                if (res.status_code !== 200 || !res.data) return;
                handleStatus(res.data);
            })
            .catch(function (err) {
                console.error('check status failed:', err);
            });
    }

    function handleStatus(data) {
        if (settled) return true;

        switch (data.status) {
            case STATUS_CONFIRMING:
                /* 已检测到付款：停掉倒计时（不能再弹超时），但继续轮询等确认结果 */
                orderData = data;
                stopCountdown();
                showConfirmingModal();
                return true;
            case STATUS_SUCCESS:
                settle();
                hideConfirmingModal();
                showSuccessModal(data);
                return true;
            case STATUS_CANCELED:
                settle();
                showCanceledModal(data);
                return true;
            case STATUS_EXPIRED:
                settle();
                showTimeoutModal(data);
                return true;
            case STATUS_FAILED:
                settle();
                showFailedModal(data);
                return true;
            default:
                return false;
        }
    }

    function settle() {
        settled = true;
        stopCountdown();
        stopStatusPolling();
    }

    /* ============================================================
       模态层
       ============================================================ */

    function buildOverlay(id, extraClass) {
        const overlay = document.createElement('div');
        overlay.className = 'status-overlay' + (extraClass ? ' ' + extraClass : '');
        if (id) overlay.id = id;
        return overlay;
    }

    function buildModal(html) {
        const modal = document.createElement('div');
        modal.className = 'sufe-modal';
        modal.innerHTML = html;
        return modal;
    }

    function returnAction(data) {
        const url = safeUrl((data && data.redirect_url) || (orderData && orderData.redirect_url) || '');
        if (!url) return '';
        return '<div class="sufe-modal-actions">' +
            '<a class="sufe-btn-primary" href="' + escapeHtml(url) + '">' +
            escapeHtml(t('payment.returnToMerchant', '返回商户平台')) + '</a></div>';
    }

    function mount(overlay, modal) {
        overlay.appendChild(modal);
        document.body.appendChild(overlay);
        return overlay;
    }

    function showTimeoutModal(data) {
        if ($id('timeout-overlay')) return;

        const overlay = buildOverlay('timeout-overlay');
        const modal = buildModal(
            '<div class="sufe-modal-icon">' +
                '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="#cc785c"' +
                ' stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round">' +
                '<circle cx="12" cy="13" r="8"/><path d="M12 9v4l2 2"/><path d="M5 3 2 6"/><path d="m22 6-3-3"/>' +
                '</svg>' +
            '</div>' +
            '<h3 class="sufe-modal-title">' + escapeHtml(t('payment.paymentTimeout', '支付时间已过期')) + '</h3>' +
            '<p class="sufe-modal-body">' + t('payment.timeoutMessage', '很抱歉，本次支付已超时。<br>请重新发起支付或联系客服处理。') + '</p>' +
            returnAction(data)
        );

        mount(overlay, modal);
    }

    function showConfirmingModal() {
        if ($id('waiting-overlay')) return;

        const overlay = buildOverlay('waiting-overlay');
        const modal = buildModal(
            '<div class="sufe-spinner"></div>' +
            '<h3 class="sufe-modal-title">' + escapeHtml(t('payment.waitingConfirmation', '等待网络确认')) + '</h3>' +
            '<p class="sufe-modal-body">' +
                '<span class="sufe-pill-success">' + escapeHtml(t('payment.confirmationMessage', '检测到付款请求，正在区块网络确认中')) + '</span><br>' +
                '<small style="color: var(--muted-soft);">' + escapeHtml(t('payment.confirmationNote', '请耐心等待，系统会自动检测确认状态')) + '</small>' +
            '</p>' +
            '<p style="font-family: var(--font-sans); font-size: 12px; color: var(--muted-soft);">' +
                escapeHtml(t('payment.estimatedTime', '预计确认时间：1–3 分钟')) +
            '</p>'
        );

        mount(overlay, modal);
    }

    function hideConfirmingModal() {
        const overlay = $id('waiting-overlay');
        if (overlay) overlay.remove();
    }

    function showSuccessModal(data) {
        if ($id('success-overlay')) return;

        const url = safeUrl(data && data.trade_url);
        let hashBlock = '';
        if (url) {
            const shown = url.length > 46 ? url.slice(0, 24) + '…' + url.slice(-16) : url;
            hashBlock =
                '<a class="sufe-modal-hash" href="' + escapeHtml(url) + '" target="_blank" rel="noopener">' +
                    '<span class="hash-label">' +
                        '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor"' +
                        ' stroke-width="2" stroke-linecap="round" stroke-linejoin="round">' +
                        '<circle cx="12" cy="12" r="10"/><line x1="2" y1="12" x2="22" y2="12"/>' +
                        '<path d="M12 2a15.3 15.3 0 0 1 4 10 15.3 15.3 0 0 1-4 10 15.3 15.3 0 0 1-4-10 15.3 15.3 0 0 1 4-10z"/>' +
                        '</svg>' +
                        escapeHtml(t('payment.onChainDetails', '链上详情')) +
                    '</span>' +
                    '<span class="hash-value">' + escapeHtml(shown) + '</span>' +
                '</a>';
        }

        const overlay = buildOverlay('success-overlay', 'success-overlay');
        const modal = buildModal(
            '<div class="sufe-modal-icon">' +
                '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="#5db872"' +
                ' stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round">' +
                '<circle cx="12" cy="12" r="10"/><path d="m9 12 2 2 4-4"/>' +
                '</svg>' +
            '</div>' +
            '<h3 class="sufe-modal-title">' + escapeHtml(t('payment.paymentSuccess', '支付已完成')) + '</h3>' +
            '<p class="sufe-modal-body">' + escapeHtml(t('payment.paymentSuccessNote', '您的付款已确认，交易已完成')) + '</p>' +
            hashBlock +
            returnAction(data)
        );

        mount(overlay, modal);
        showToast(t('payment.paymentSuccess', '支付已完成'), 'success');
    }

    function showCanceledModal(data) {
        if ($id('canceled-overlay')) return;

        const overlay = buildOverlay('canceled-overlay');
        const modal = buildModal(
            '<div class="sufe-modal-icon" style="background: rgba(20,20,19,0.06);">' +
                '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="#6c6a64"' +
                ' stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round">' +
                '<circle cx="12" cy="12" r="10"/><path d="M15 9l-6 6"/><path d="M9 9l6 6"/>' +
                '</svg>' +
            '</div>' +
            '<h3 class="sufe-modal-title">' + escapeHtml(t('payment.orderCanceled', '订单已取消')) + '</h3>' +
            '<p class="sufe-modal-body">' + t('payment.canceledMessage', '该订单已取消，无法继续付款。<br>如需支付请重新下单。') + '</p>' +
            returnAction(data)
        );

        mount(overlay, modal);
        showToast(t('payment.orderCanceled', '订单已取消'), 'error');
    }

    function showFailedModal(data) {
        if ($id('failed-overlay')) return;

        const overlay = buildOverlay('failed-overlay');
        const modal = buildModal(
            '<div class="sufe-modal-icon" style="background: rgba(198,69,69,0.12);">' +
                '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="#c64545"' +
                ' stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round">' +
                '<circle cx="12" cy="12" r="10"/><line x1="12" y1="8" x2="12" y2="13"/><line x1="12" y1="16.5" x2="12" y2="16.51"/>' +
                '</svg>' +
            '</div>' +
            '<h3 class="sufe-modal-title">' + escapeHtml(t('payment.paymentFailed', '交易确认失败')) + '</h3>' +
            '<p class="sufe-modal-body">' + t('payment.failedMessage', '本次交易确认失败。<br>请联系商户客服核实处理。') + '</p>' +
            returnAction(data)
        );

        mount(overlay, modal);
    }

    /* ============================================================
       数据加载
       ============================================================ */

    function showLoadError(message) {
        hide(dom.selectorSection);
        hide(dom.qrSection);
        show(dom.skeleton);

        if (dom.skeleton) {
            dom.skeleton.innerHTML = '';
            const text = document.createElement('span');
            text.className = 'stage-skeleton-text';
            text.style.color = 'var(--error)';
            text.textContent = message;
            dom.skeleton.appendChild(text);
        }

        showToast(message, 'error');
    }

    function loadOrder() {
        return apiPost('/api/v1/pay/info', { trade_id: tradeId })
            .then(function (res) {
                if (res.status_code !== 200 || !res.data) {
                    showLoadError(res.message || t('payment.loadOrderFailed', '订单加载失败'));
                    return;
                }

                orderData = res.data;
                renderOrderBase(orderData);
                hide(dom.skeleton);

                const status = orderData.status;

                /* 终态订单：先把底稿画出来，再把状态模态盖上去，不再启动任何定时器 */
                if (status === STATUS_SUCCESS || status === STATUS_CANCELED ||
                    status === STATUS_EXPIRED || status === STATUS_FAILED) {
                    if (orderData.token) showPaymentStage(orderData);
                    handleStatus(orderData);
                    return;
                }

                /* 确认中：不跑倒计时，只轮询等最终结果 */
                if (status === STATUS_CONFIRMING) {
                    if (orderData.token) showPaymentStage(orderData);
                    showConfirmingModal();
                    startStatusPolling();
                    return;
                }

                startCountdown(orderData.expired_at, orderData.created_at);
                startStatusPolling();

                if (!orderData.trade_type || !orderData.token) showSelectionStage();
                else showPaymentStage(orderData);
            })
            .catch(function (err) {
                console.error('load order failed:', err);
                showLoadError(t('payment.networkError', '网络异常，请稍后重试'));
            });
    }

    function loadMethods() {
        return apiPost('/api/v1/pay/methods', { trade_id: tradeId })
            .then(function (res) {
                if (res.status_code !== 200 || !res.data || !Array.isArray(res.data.methods) || !res.data.methods.length) {
                    paymentMethods = [];
                    networkSort = '';
                    showToast(res.message || t('payment.loadPaymentNetworkFailed', '未能加载付款网络，请确认收款钱包已经配置完成'), 'error');
                    return;
                }

                paymentMethods = res.data.methods;
                networkSort = typeof res.data.network_sort === 'string' ? res.data.network_sort : '';

                buildCurrencyOptions();
                setSelectEnabled(dom.networkSelect, !!selectedCurrency);
                refreshSelectionView();
            })
            .catch(function (err) {
                console.error('load methods failed:', err);
                showToast(t('payment.networkError', '网络异常，请稍后重试'), 'error');
            });
    }

    function createTransaction() {
        if (!selectedMethod || settled) return;
        if (dom.payBtn && dom.payBtn.disabled) return;

        if (orderData && orderData.status && orderData.status !== STATUS_WAITING) {
            showToast(t('payment.orderNotPayable', '当前订单状态不允许继续付款'), 'error');
            return;
        }

        if (dom.payBtn) {
            dom.payBtn.disabled = true;
            dom.payBtn.classList.add('is-loading');
        }

        apiPost('/api/v1/pay/update-order', {
            trade_id: tradeId,
            currency: selectedMethod.currency,
            network: selectedMethod.network
        })
            .then(function (res) {
                if (res.status_code === 200 && res.data && res.data.payment_url) {
                    window.location.href = res.data.payment_url;
                    return;
                }
                releasePayBtn();
                showToast(res.message || t('payment.createTransactionFailed', '创建交易失败，请稍后再试'), 'error');
            })
            .catch(function (err) {
                console.error('create transaction failed:', err);
                releasePayBtn();
                showToast(t('payment.networkError', '网络异常，请稍后重试'), 'error');
            });
    }

    function releasePayBtn() {
        if (!dom.payBtn) return;
        dom.payBtn.classList.remove('is-loading');
        dom.payBtn.disabled = !selectedMethod;
    }

    /* 已确认付款方式但后台允许重选 —— 无需刷新页面，直接回到阶段一 */
    function backToSelection() {
        if (settled || !orderData) return;

        selectedCurrency = '';
        selectedMethod = null;
        resetTrigger(dom.currencySelect, 'payment.selectCurrency', '请选择币种');
        resetTrigger(dom.networkSelect, 'payment.selectNetwork', '请选择网络');
        setSelectEnabled(dom.networkSelect, false);
        showSelectionStage();
    }

    /* ============================================================
       启动
       ============================================================ */

    function cacheDom() {
        dom.toast = $id('sufeToast');
        dom.supportBtn = $id('supportBtn');
        dom.languageSwitcher = $id('languageSwitcher');

        dom.heroTokenLogo = $id('heroTokenLogo');
        dom.heroIconText = $id('heroIconText');
        dom.payTitle = $id('payTitle');
        dom.paySubtitle = $id('paySubtitle');
        dom.netBadge = $id('netBadge');

        dom.payAmount = $id('payAmount');
        dom.amountNum = dom.payAmount ? dom.payAmount.querySelector('.amount-num') : null;
        dom.orderId = $id('orderId');
        dom.orderName = $id('orderName');
        dom.orderNameRow = $id('orderNameRow');
        dom.orderMoney = $id('orderMoney');

        dom.countdownBanner = $id('countdownBanner');
        dom.hourUnit = $id('hourUnit');
        dom.hourSeparator = $id('hourSeparator');
        dom.hours = $id('hours');
        dom.minutes = $id('minutes');
        dom.seconds = $id('seconds');

        dom.skeleton = $id('stageSkeleton');
        dom.selectorSection = $id('selectorSection');
        dom.currencySelect = $id('currencySelect');
        dom.networkSelect = $id('networkSelect');
        dom.payBtn = $id('payBtn');

        dom.qrSection = $id('qrSection');
        dom.qrCodeBox = document.querySelector('#qrSection .qr-code');
        dom.qrcode = $id('qrcode');
        dom.qrLogoBadge = $id('qrLogoBadge');
        dom.qrTokenLogo = $id('qrTokenLogo');
        dom.qrNetworkLogo = $id('qrNetworkLogo');
        dom.addressLabel = $id('addressLabel');
        dom.walletAddress = $id('walletAddress');
        dom.copyAddressBtn = $id('copyAddressBtn');
        dom.reselectBtn = $id('reselectBtn');

        dom.instrSelection = $id('instrSelection');
        dom.instrPayment = $id('instrPayment');
    }

    function bindEvents() {
        document.addEventListener('click', function () {
            closeAllDropdowns();
        });

        document.addEventListener('keydown', function (e) {
            if (e.key === 'Escape') closeAllDropdowns();
        });

        bindSelectTrigger(dom.currencySelect);
        bindSelectTrigger(dom.networkSelect);
        setSelectEnabled(dom.networkSelect, false);

        if (dom.payBtn) dom.payBtn.addEventListener('click', createTransaction);
        if (dom.reselectBtn) dom.reselectBtn.addEventListener('click', backToSelection);

        let resizeTimer = null;
        window.addEventListener('resize', function () {
            clearTimeout(resizeTimer);
            resizeTimer = setTimeout(function () {
                if (stage === 'payment' && orderData) renderQr(orderData.token || '');
            }, 200);
        });

        /* 切回前台时立刻补一次状态查询，避免手机息屏期间漏掉终态 */
        document.addEventListener('visibilitychange', function () {
            if (!document.hidden && !settled) checkStatus();
        });
    }

    function init(config) {
        const cfg = config || {};
        tradeId = String(cfg.trade_id || '').trim();

        cacheDom();
        bindEvents();

        if (!tradeId) {
            showLoadError('missing trade_id');
            return;
        }

        initI18n().then(loadOrder);
    }

    window.Payment = {
        init: init,
        copyAmount: copyAmount,
        copyAddress: copyAddress,
        changeLanguage: changeLanguage,
        t: t
    };

    /* 模板里以 onclick / onchange 直接调用 */
    window.copyAmount = copyAmount;
    window.copyAddress = copyAddress;
    window.changeLanguage = changeLanguage;
})();
