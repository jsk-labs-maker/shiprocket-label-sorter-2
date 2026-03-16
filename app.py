"""
Shiprocket Label Sorter 2.0 - Web Interface
=============================================
Upload bulk labels → Get sorted PDFs by Courier + SKU
Duplicate order detection by Customer Contact Number

Built by Kluzo 😎 for JSK Labs
"""

import streamlit as st
import re
import io
import csv
import zipfile
from collections import defaultdict, Counter
from datetime import datetime
from pypdf import PdfReader, PdfWriter

st.set_page_config(
    page_title="Label Sorter 2.0 | JSK Labs",
    page_icon="📦",
    layout="wide"
)

# --- Custom CSS ---
st.markdown("""
<style>
    /* Import Google Fonts */
    @import url('https://fonts.googleapis.com/css2?family=DM+Sans:wght@400;500;700&family=JetBrains+Mono:wght@400;500&display=swap');

    /* Global */
    .stApp {
        font-family: 'DM Sans', sans-serif;
    }
    
    /* Header area */
    .hero-container {
        background: linear-gradient(135deg, #0f172a 0%, #1e293b 50%, #334155 100%);
        border-radius: 16px;
        padding: 2rem 2.5rem;
        margin-bottom: 1.5rem;
        border: 1px solid #334155;
        position: relative;
        overflow: hidden;
    }
    .hero-container::before {
        content: '';
        position: absolute;
        top: -50%;
        right: -20%;
        width: 300px;
        height: 300px;
        background: radial-gradient(circle, rgba(99,102,241,0.15) 0%, transparent 70%);
        border-radius: 50%;
    }
    .hero-title {
        font-family: 'DM Sans', sans-serif;
        font-size: 2rem;
        font-weight: 700;
        color: #f8fafc;
        margin: 0 0 0.3rem 0;
        letter-spacing: -0.5px;
    }
    .hero-subtitle {
        font-family: 'DM Sans', sans-serif;
        font-size: 0.95rem;
        color: #94a3b8;
        margin: 0;
        font-weight: 400;
    }
    .hero-badge {
        display: inline-block;
        background: linear-gradient(135deg, #6366f1, #8b5cf6);
        color: white;
        padding: 0.2rem 0.7rem;
        border-radius: 20px;
        font-size: 0.7rem;
        font-weight: 700;
        letter-spacing: 0.5px;
        text-transform: uppercase;
        margin-bottom: 0.7rem;
    }

    /* Metric cards */
    .metric-card {
        background: #ffffff;
        border: 1px solid #e2e8f0;
        border-radius: 12px;
        padding: 1.2rem 1.5rem;
        text-align: center;
        transition: all 0.2s ease;
        box-shadow: 0 1px 3px rgba(0,0,0,0.04);
    }
    .metric-card:hover {
        border-color: #6366f1;
        box-shadow: 0 4px 12px rgba(99,102,241,0.1);
        transform: translateY(-1px);
    }
    .metric-value {
        font-family: 'JetBrains Mono', monospace;
        font-size: 2rem;
        font-weight: 700;
        color: #0f172a;
        line-height: 1;
        margin-bottom: 0.3rem;
    }
    .metric-label {
        font-size: 0.8rem;
        color: #64748b;
        font-weight: 500;
        text-transform: uppercase;
        letter-spacing: 0.5px;
    }
    .metric-value.text-indigo { color: #6366f1; }
    .metric-value.text-emerald { color: #10b981; }
    .metric-value.text-amber { color: #f59e0b; }
    .metric-value.text-rose { color: #f43f5e; }

    /* Result row cards */
    .result-row {
        background: #f8fafc;
        border: 1px solid #e2e8f0;
        border-radius: 10px;
        padding: 0.8rem 1.2rem;
        margin-bottom: 0.5rem;
        display: flex;
        align-items: center;
        justify-content: space-between;
        transition: all 0.15s ease;
    }
    .result-row:hover {
        background: #f1f5f9;
        border-color: #cbd5e1;
    }
    .result-courier {
        font-weight: 700;
        color: #0f172a;
        font-size: 0.95rem;
    }
    .result-sku {
        color: #6366f1;
        font-family: 'JetBrains Mono', monospace;
        font-size: 0.85rem;
        font-weight: 500;
    }
    .result-count {
        background: #0f172a;
        color: white;
        padding: 0.25rem 0.75rem;
        border-radius: 20px;
        font-family: 'JetBrains Mono', monospace;
        font-size: 0.8rem;
        font-weight: 500;
    }
    .result-date {
        color: #94a3b8;
        font-size: 0.8rem;
    }
    
    /* Duplicate warning card */
    .dup-card {
        background: linear-gradient(135deg, #fef3c7 0%, #fde68a 100%);
        border: 1px solid #f59e0b;
        border-radius: 12px;
        padding: 1rem 1.5rem;
        margin: 1rem 0;
    }
    .dup-card-danger {
        background: linear-gradient(135deg, #fee2e2 0%, #fecaca 100%);
        border: 1px solid #f43f5e;
    }
    .dup-title {
        font-weight: 700;
        color: #92400e;
        font-size: 1rem;
        margin-bottom: 0.3rem;
    }
    .dup-card-danger .dup-title { color: #9f1239; }
    .dup-detail {
        color: #78350f;
        font-size: 0.85rem;
    }
    .dup-card-danger .dup-detail { color: #881337; }

    /* Upload area */
    [data-testid="stFileUploader"] {
        border-radius: 12px;
    }
    
    /* Section headers */
    .section-header {
        font-family: 'DM Sans', sans-serif;
        font-size: 1.1rem;
        font-weight: 700;
        color: #0f172a;
        margin: 1.5rem 0 0.8rem 0;
        padding-bottom: 0.5rem;
        border-bottom: 2px solid #e2e8f0;
    }
    
    /* Footer */
    .footer {
        text-align: center;
        padding: 2rem 0 1rem 0;
        color: #94a3b8;
        font-size: 0.8rem;
    }
    .footer a {
        color: #6366f1;
        text-decoration: none;
        font-weight: 500;
    }
    
    /* Courier badge colors */
    .courier-badge {
        display: inline-block;
        padding: 0.15rem 0.6rem;
        border-radius: 6px;
        font-size: 0.75rem;
        font-weight: 600;
        letter-spacing: 0.3px;
    }
    .courier-ekart { background: #dbeafe; color: #1e40af; }
    .courier-delhivery { background: #fce7f3; color: #9d174d; }
    .courier-xpressbees { background: #d1fae5; color: #065f46; }
    .courier-shadowfax { background: #ede9fe; color: #5b21b6; }
    .courier-bluedart { background: #e0f2fe; color: #075985; }
    .courier-dtdc { background: #fee2e2; color: #991b1b; }
    .courier-ecomexpress { background: #fef9c3; color: #854d0e; }
    .courier-unknown { background: #f1f5f9; color: #475569; }

    /* Hide default streamlit padding */
    .block-container { padding-top: 2rem; }
    
    /* Table styling */
    .dup-table {
        width: 100%;
        border-collapse: separate;
        border-spacing: 0;
        font-size: 0.85rem;
        border-radius: 8px;
        overflow: hidden;
        border: 1px solid #e2e8f0;
    }
    .dup-table th {
        background: #0f172a;
        color: #f8fafc;
        padding: 0.6rem 1rem;
        text-align: left;
        font-weight: 600;
        font-size: 0.75rem;
        text-transform: uppercase;
        letter-spacing: 0.5px;
    }
    .dup-table td {
        padding: 0.5rem 1rem;
        border-bottom: 1px solid #f1f5f9;
        color: #334155;
    }
    .dup-table tr:nth-child(even) td {
        background: #f8fafc;
    }
    .dup-table tr:hover td {
        background: #eef2ff;
    }
    .phone-mono {
        font-family: 'JetBrains Mono', monospace;
        font-weight: 500;
        color: #6366f1;
    }
</style>
""", unsafe_allow_html=True)


# --- Helper Functions ---

def normalize_sku(sku_raw: str) -> str:
    """Normalize SKU for filename safety."""
    return re.sub(r'[^\w\-]', '', sku_raw.replace(' ', '-'))[:50]


def get_courier_badge(courier: str) -> str:
    """Return HTML for a styled courier badge."""
    css_class = f"courier-{courier.lower()}"
    return f'<span class="courier-badge {css_class}">{courier}</span>'


def extract_label_info(page_text: str) -> dict:
    """
    Extract courier, SKU, date, order ID, and phone from Shiprocket label text.
    """
    info = {
        'courier': 'Unknown',
        'sku': 'Unknown',
        'date': datetime.now().strftime('%Y-%m-%d'),
        'order_id': None,
        'phone': None,
        'customer_name': None,
    }
    
    # --- COURIER ---
    courier_patterns = [
        (r'Ekart', 'Ekart'),
        (r'Delhivery', 'Delhivery'),
        (r'Xpressbees', 'Xpressbees'),
        (r'BlueDart', 'BlueDart'),
        (r'DTDC', 'DTDC'),
        (r'Shadowfax', 'Shadowfax'),
        (r'Ecom\s*Express', 'EcomExpress'),
    ]
    for pattern, name in courier_patterns:
        if re.search(pattern, page_text, re.IGNORECASE):
            info['courier'] = name
            break
    
    # --- SKU (from table block) ---
    sku_block = re.search(r'Item\nSKU\nQty\nPrice\nTotal\n(.+?)\n(\d+)\n₹', page_text, re.DOTALL)
    if sku_block:
        block_lines = sku_block.group(1).strip().split('\n')
        sku_lines = []
        for line in reversed(block_lines):
            stripped = line.strip()
            if '...' in stripped or len(stripped) > 40 or '|' in stripped:
                break
            sku_lines.insert(0, stripped)
        if sku_lines:
            info['sku'] = normalize_sku(' '.join(sku_lines))
    
    # --- DATE (DD/MM/YYYY → YYYY-MM-DD) ---
    date_match = re.search(r'Invoice Date:\s*(\d{2}/\d{2}/\d{4})', page_text)
    if date_match:
        try:
            parsed = datetime.strptime(date_match.group(1), '%d/%m/%Y')
            info['date'] = parsed.strftime('%Y-%m-%d')
        except ValueError:
            pass
    else:
        date_match = re.search(r'Invoice Date:\s*(\d{4}-\d{2}-\d{2})', page_text)
        if date_match:
            info['date'] = date_match.group(1)
    
    # --- ORDER ID ---
    order_match = re.search(r'Order#:\s*(\d+)', page_text)
    if order_match:
        info['order_id'] = order_match.group(1).strip()
    
    # --- PHONE ---
    ship_to_block = re.search(r'Ship To\n(.+?)Dimensions:', page_text, re.DOTALL)
    if ship_to_block:
        phone_match = re.search(r'\b(\d{10})\b', ship_to_block.group(1))
        if phone_match:
            phone = phone_match.group(1)
            if phone[0] in '6789':
                info['phone'] = phone
    
    # --- CUSTOMER NAME ---
    name_match = re.search(r'Ship To\n([^\n]+)', page_text)
    if name_match:
        name = name_match.group(1).strip()
        if name and not name.isdigit():
            info['customer_name'] = name
    
    return info


def sort_labels(pdf_file, filter_duplicates: bool = True) -> tuple:
    """Sort labels and detect duplicates by phone number."""
    reader = PdfReader(pdf_file)
    total_pages = len(reader.pages)
    
    all_labels = []
    blank_pages = []
    progress_bar = st.progress(0, text="Scanning labels...")
    
    for i, page in enumerate(reader.pages):
        text = page.extract_text() or ''
        
        # Skip blank/empty pages (common in merged PDFs)
        if len(text.strip()) < 20:
            blank_pages.append(i)
            progress_bar.progress((i + 1) / total_pages, text=f"Scanning label {i+1} of {total_pages}")
            continue
        
        info = extract_label_info(text)
        info['page_index'] = i
        all_labels.append(info)
        progress_bar.progress((i + 1) / total_pages, text=f"Scanning label {i+1} of {total_pages}")
    
    progress_bar.progress(1.0, text="Detecting duplicate contacts...")
    
    actual_labels = len(all_labels)
    
    # Detect duplicates by phone
    phone_order_map = defaultdict(list)
    for idx, label in enumerate(all_labels):
        if label['phone']:
            phone_order_map[label['phone']].append(idx)
    
    duplicate_phones = {phone: indices for phone, indices in phone_order_map.items() if len(indices) > 1}
    
    # Filter duplicates
    duplicate_page_indices = set()
    if filter_duplicates and duplicate_phones:
        for phone, indices in duplicate_phones.items():
            for dup_idx in indices[1:]:
                duplicate_page_indices.add(all_labels[dup_idx]['page_index'])
    
    # Group pages (skip blank + skip duplicates)
    groups = defaultdict(list)
    for label in all_labels:
        if label['page_index'] not in duplicate_page_indices:
            key = (label['date'], label['courier'], label['sku'])
            groups[key].append(label['page_index'])
    
    progress_bar.progress(1.0, text="Building sorted PDFs...")
    
    # Create zip
    zip_buffer = io.BytesIO()
    results = []
    
    with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zf:
        for (date, courier, sku), page_indices in sorted(groups.items()):
            filename = f"{date}_{courier}_{sku}.pdf"
            writer = PdfWriter()
            for idx in page_indices:
                writer.add_page(reader.pages[idx])
            pdf_buffer = io.BytesIO()
            writer.write(pdf_buffer)
            pdf_buffer.seek(0)
            zf.writestr(filename, pdf_buffer.getvalue())
            results.append({
                'file': filename, 'date': date,
                'courier': courier, 'sku': sku,
                'labels': len(page_indices)
            })
        
        if duplicate_page_indices:
            dup_writer = PdfWriter()
            for page_idx in sorted(duplicate_page_indices):
                dup_writer.add_page(reader.pages[page_idx])
            dup_pdf_buffer = io.BytesIO()
            dup_writer.write(dup_pdf_buffer)
            dup_pdf_buffer.seek(0)
            zf.writestr("_DUPLICATE_ORDERS.pdf", dup_pdf_buffer.getvalue())
        
        if duplicate_phones:
            csv_buffer = io.StringIO()
            writer_csv = csv.writer(csv_buffer)
            writer_csv.writerow(['Phone Number', 'Occurrences', 'Order IDs', 'Customer Names', 'SKUs', 'Couriers'])
            for phone, indices in sorted(duplicate_phones.items(), key=lambda x: len(x[1]), reverse=True):
                labels = [all_labels[i] for i in indices]
                order_ids = ', '.join(filter(None, [l.get('order_id', '') for l in labels]))
                names = ', '.join(filter(None, [l.get('customer_name', '') for l in labels]))
                skus = ', '.join(set(filter(None, [l.get('sku', '') for l in labels])))
                couriers = ', '.join(set(filter(None, [l.get('courier', '') for l in labels])))
                writer_csv.writerow([phone, len(indices), order_ids, names, skus, couriers])
            zf.writestr("_DUPLICATE_CONTACTS.csv", csv_buffer.getvalue())
    
    zip_buffer.seek(0)
    progress_bar.empty()
    
    return zip_buffer, results, total_pages, {
        'duplicate_phone_count': len(duplicate_phones),
        'duplicate_labels_removed': len(duplicate_page_indices),
        'duplicate_phones': duplicate_phones,
        'all_labels': all_labels,
        'blank_pages': len(blank_pages),
        'actual_labels': actual_labels,
    }


# ============================================================
# UI
# ============================================================

# --- Hero Header ---
st.markdown("""
<div class="hero-container">
    <div class="hero-badge">v2.0</div>
    <h1 class="hero-title">📦 Shiprocket Label Sorter</h1>
    <p class="hero-subtitle">Sort bulk labels by Courier + SKU &nbsp;·&nbsp; Detect duplicate orders by Contact Number</p>
</div>
""", unsafe_allow_html=True)

# --- Upload Section ---
col_upload, col_options = st.columns([3, 2])

with col_upload:
    uploaded_file = st.file_uploader(
        "Drop your Shiprocket bulk labels PDF here",
        type=['pdf'],
        help="Download bulk labels from Shiprocket and upload here"
    )

with col_options:
    if uploaded_file:
        file_size = uploaded_file.size
        if file_size > 1048576:
            size_str = f"{file_size / 1048576:.1f} MB"
        else:
            size_str = f"{file_size / 1024:.1f} KB"
        
        st.markdown(f"""
        <div class="metric-card" style="margin-top: 0.5rem;">
            <div class="metric-value text-indigo" style="font-size:1.3rem;">📄 {uploaded_file.name}</div>
            <div class="metric-label">{size_str}</div>
        </div>
        """, unsafe_allow_html=True)
        
        filter_dupes = st.toggle("🔍 Remove duplicate orders", value=True, 
                                  help="Same phone number in multiple orders → keeps first, removes rest")
    else:
        st.markdown("""
        <div class="metric-card" style="margin-top: 0.5rem; opacity: 0.6;">
            <div class="metric-value" style="font-size:1.3rem; color: #94a3b8;">No file selected</div>
            <div class="metric-label">Upload a PDF to get started</div>
        </div>
        """, unsafe_allow_html=True)
        filter_dupes = True

# --- Process Button ---
if uploaded_file:
    if st.button("🚀  Sort & Analyze Labels", type="primary", use_container_width=True):
        with st.spinner(""):
            try:
                zip_buffer, results, total_pages, dup_info = sort_labels(uploaded_file, filter_duplicates=filter_dupes)
                all_labels = dup_info['all_labels']
                actual_labels = dup_info['actual_labels']
                blank_pages = dup_info['blank_pages']
                
                # --- Metrics Row ---
                st.markdown("")
                m1, m2, m3, m4 = st.columns(4)
                
                courier_count = len(set(r['courier'] for r in results))
                sku_count = len(set(r['sku'] for r in results))
                
                with m1:
                    blank_note = f'<div style="font-size:0.65rem;color:#94a3b8;margin-top:2px;">{blank_pages} blank skipped</div>' if blank_pages > 0 else ''
                    st.markdown(f"""
                    <div class="metric-card">
                        <div class="metric-value text-indigo">{actual_labels}</div>
                        <div class="metric-label">Total Labels</div>
                        {blank_note}
                    </div>""", unsafe_allow_html=True)
                with m2:
                    st.markdown(f"""
                    <div class="metric-card">
                        <div class="metric-value text-emerald">{len(results)}</div>
                        <div class="metric-label">Output Files</div>
                    </div>""", unsafe_allow_html=True)
                with m3:
                    st.markdown(f"""
                    <div class="metric-card">
                        <div class="metric-value text-amber">{courier_count}</div>
                        <div class="metric-label">Couriers</div>
                    </div>""", unsafe_allow_html=True)
                with m4:
                    dup_count = dup_info['duplicate_phone_count']
                    color = "text-rose" if dup_count > 0 else "text-emerald"
                    st.markdown(f"""
                    <div class="metric-card">
                        <div class="metric-value {color}">{dup_count}</div>
                        <div class="metric-label">Duplicate Contacts</div>
                    </div>""", unsafe_allow_html=True)
                
                st.markdown("")
                
                # --- Duplicate Alert ---
                if dup_info['duplicate_phone_count'] > 0:
                    action = "removed" if filter_dupes else "detected (kept in output)"
                    st.markdown(f"""
                    <div class="dup-card {'dup-card-danger' if dup_info['duplicate_labels_removed'] > 5 else ''}">
                        <div class="dup-title">⚠️ {dup_info['duplicate_phone_count']} Duplicate Contact{'s' if dup_info['duplicate_phone_count'] > 1 else ''} Found — {dup_info['duplicate_labels_removed']} extra label{'s' if dup_info['duplicate_labels_removed'] != 1 else ''} {action}</div>
                        <div class="dup-detail">Orders with the same phone number across multiple shipments. Check _DUPLICATE_CONTACTS.csv in the ZIP for full details.</div>
                    </div>
                    """, unsafe_allow_html=True)
                    
                    # Duplicate table
                    with st.expander(f"📱 View all {dup_info['duplicate_phone_count']} duplicate contacts"):
                        table_rows = ""
                        for phone, indices in sorted(dup_info['duplicate_phones'].items(), key=lambda x: len(x[1]), reverse=True):
                            labels = [all_labels[i] for i in indices]
                            order_ids = ', '.join(filter(None, [l.get('order_id', '—') for l in labels]))
                            names = ', '.join(filter(None, [l.get('customer_name', '—') for l in labels]))
                            couriers_html = ' '.join(get_courier_badge(l.get('courier', 'Unknown')) for l in labels)
                            table_rows += f"""
                            <tr>
                                <td><span class="phone-mono">{phone}</span></td>
                                <td><strong>{len(indices)}</strong></td>
                                <td style="font-family:'JetBrains Mono',monospace; font-size:0.8rem;">{order_ids}</td>
                                <td>{names}</td>
                                <td>{couriers_html}</td>
                            </tr>"""
                        
                        st.markdown(f"""
                        <table class="dup-table">
                            <thead>
                                <tr>
                                    <th>Phone</th>
                                    <th>Count</th>
                                    <th>Order IDs</th>
                                    <th>Customer Names</th>
                                    <th>Couriers</th>
                                </tr>
                            </thead>
                            <tbody>{table_rows}</tbody>
                        </table>
                        """, unsafe_allow_html=True)
                
                # --- Sorted Files ---
                st.markdown('<div class="section-header">📊 Sorted Output Files</div>', unsafe_allow_html=True)
                
                for r in results:
                    courier_badge = get_courier_badge(r['courier'])
                    sku_display = r['sku'].replace('-', ' ')
                    st.markdown(f"""
                    <div class="result-row">
                        <div>
                            {courier_badge}
                            <span class="result-sku" style="margin-left: 0.5rem;">{sku_display}</span>
                        </div>
                        <div style="display: flex; align-items: center; gap: 1rem;">
                            <span class="result-date">{r['date']}</span>
                            <span class="result-count">{r['labels']} labels</span>
                        </div>
                    </div>
                    """, unsafe_allow_html=True)
                
                # --- Courier Breakdown ---
                st.markdown("")
                st.markdown('<div class="section-header">🚚 Courier Breakdown</div>', unsafe_allow_html=True)
                
                courier_counts = Counter()
                for r in results:
                    courier_counts[r['courier']] += r['labels']
                
                cols = st.columns(len(courier_counts))
                for i, (courier, count) in enumerate(courier_counts.most_common()):
                    with cols[i]:
                        st.markdown(f"""
                        <div class="metric-card">
                            <div class="metric-value" style="font-size:1.5rem;">{count}</div>
                            <div class="metric-label">{courier}</div>
                        </div>""", unsafe_allow_html=True)
                
                # --- Download ---
                st.markdown("")
                st.markdown("")
                
                dl_col1, dl_col2, dl_col3 = st.columns([1, 2, 1])
                with dl_col2:
                    st.download_button(
                        label="📥  Download Sorted Labels (ZIP)",
                        data=zip_buffer,
                        file_name=f"sorted_labels_{datetime.now().strftime('%Y%m%d_%H%M%S')}.zip",
                        mime="application/zip",
                        type="primary",
                        use_container_width=True
                    )
                    if dup_info['duplicate_phone_count'] > 0:
                        st.caption("ZIP includes: sorted PDFs + `_DUPLICATE_ORDERS.pdf` + `_DUPLICATE_CONTACTS.csv`")
                    else:
                        st.caption("ZIP includes all sorted PDFs organized by Date, Courier & SKU")
                
            except Exception as e:
                st.error(f"❌ Error: {str(e)}")
                st.exception(e)

# --- How it works (bottom) ---
st.markdown("")
st.markdown("")

with st.expander("ℹ️  How it works"):
    c1, c2, c3 = st.columns(3)
    with c1:
        st.markdown("""
        **1. Upload**  
        Drop your Shiprocket bulk labels PDF. The tool reads every page and extracts courier, SKU, date, order ID, and phone number.
        """)
    with c2:
        st.markdown("""
        **2. Detect Duplicates**  
        Finds orders with the same contact number. Duplicate labels are separated into `_DUPLICATE_ORDERS.pdf` and listed in `_DUPLICATE_CONTACTS.csv`.
        """)
    with c3:
        st.markdown("""
        **3. Download**  
        Get a ZIP with PDFs sorted by `Date_Courier_SKU.pdf`. Supports Ekart, Delhivery, Xpressbees, Shadowfax, BlueDart, DTDC, Ecom Express.
        """)

# --- Footer ---
st.markdown("""
<div class="footer">
    Built with ❤️ by <strong>JSK Labs</strong> &nbsp;·&nbsp; Powered by Kluzo 😎
</div>
""", unsafe_allow_html=True)
