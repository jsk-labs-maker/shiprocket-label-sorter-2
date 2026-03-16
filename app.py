"""
Shiprocket Label Sorter 2.0 - Web Interface
=============================================
Upload bulk labels → Get sorted PDFs by Courier + SKU
Now with Duplicate Order Filter & Duplicate Contact Detection

Built by Kluzo 😎 for JSK Labs
"""

import streamlit as st
import re
import io
import csv
import zipfile
from collections import defaultdict
from datetime import datetime
from pypdf import PdfReader, PdfWriter

st.set_page_config(
    page_title="Label Sorter 2.0 | JSK Labs",
    page_icon="📦",
    layout="centered"
)

# --- Helper Functions ---

def normalize_courier(courier_raw: str) -> str:
    """Normalize courier names for consistent file naming."""
    courier_lower = courier_raw.lower()
    
    if 'ekart' in courier_lower:
        return 'Ekart'
    elif 'delhivery' in courier_lower:
        return 'Delhivery'
    elif 'xpressbees' in courier_lower:
        return 'Xpressbees'
    elif 'bluedart' in courier_lower:
        return 'BlueDart'
    elif 'dtdc' in courier_lower:
        return 'DTDC'
    elif 'shadowfax' in courier_lower:
        return 'Shadowfax'
    elif 'ecom' in courier_lower:
        return 'EcomExpress'
    else:
        return re.sub(r'[^\w\-]', '', courier_raw.replace(' ', '-'))[:30]


def normalize_sku(sku_raw: str) -> str:
    """Normalize SKU for filename safety."""
    return re.sub(r'[^\w\-]', '', sku_raw.replace(' ', '-'))[:50]


def extract_label_info(page_text: str) -> dict:
    """Extract courier, SKU, date, order ID, and phone from label text."""
    info = {
        'courier': 'Unknown',
        'sku': 'Unknown',
        'date': datetime.now().strftime('%Y-%m-%d'),
        'order_id': None,
        'phone': None,
        'customer_name': None,
    }
    
    courier_patterns = [
        (r'Ekart[^\n]*', 'Ekart'),
        (r'Delhivery[^\n]*', 'Delhivery'),
        (r'Xpressbees[^\n]*', 'Xpressbees'),
        (r'BlueDart[^\n]*', 'BlueDart'),
        (r'DTDC[^\n]*', 'DTDC'),
        (r'Shadowfax[^\n]*', 'Shadowfax'),
        (r'Ecom\s*Express[^\n]*', 'EcomExpress'),
    ]
    
    for pattern, name in courier_patterns:
        match = re.search(pattern, page_text, re.IGNORECASE)
        if match:
            info['courier'] = name
            break
    
    sku_match = re.search(r'SKU:\s*([^\n]+)', page_text)
    if sku_match:
        info['sku'] = normalize_sku(sku_match.group(1).strip())
    
    date_match = re.search(r'Invoice Date:\s*(\d{4}-\d{2}-\d{2})', page_text)
    if date_match:
        info['date'] = date_match.group(1)
    else:
        date_match = re.search(r'(\d{4}-\d{2}-\d{2})', page_text)
        if date_match:
            info['date'] = date_match.group(1)
    
    # Order ID extraction (Shiprocket order IDs / Order No)
    order_patterns = [
        r'Order\s*(?:No|ID|#)[.:;]?\s*([A-Za-z0-9\-_]+)',
        r'Order[:\s]+([A-Za-z0-9\-_]+)',
        r'order_id[:\s]+([A-Za-z0-9\-_]+)',
    ]
    for pat in order_patterns:
        m = re.search(pat, page_text, re.IGNORECASE)
        if m:
            info['order_id'] = m.group(1).strip()
            break
    
    # Phone number extraction (Indian 10-digit numbers)
    phone_patterns = [
        r'(?:Phone|Mobile|Contact|Tel|Ph)[.:;]?\s*\+?91?\s*[-.]?\s*(\d{10})',
        r'(?:Phone|Mobile|Contact|Tel|Ph)[.:;]?\s*(\d{10})',
        r'\b(\d{10})\b',
    ]
    for pat in phone_patterns:
        m = re.search(pat, page_text, re.IGNORECASE)
        if m:
            phone = m.group(1).strip()
            # Validate it looks like a real phone (starts with 6-9 for Indian numbers)
            if len(phone) == 10 and phone[0] in '6789':
                info['phone'] = phone
                break
    
    # Customer name extraction
    name_match = re.search(r'(?:Customer|Deliver(?:y)?\s*To|Ship\s*To|Name)[.:;]?\s*([A-Za-z ]+)', page_text, re.IGNORECASE)
    if name_match:
        info['customer_name'] = name_match.group(1).strip()
    
    return info


def sort_labels(pdf_file, filter_duplicates: bool = True) -> tuple:
    """Sort labels and return zip buffer with results, duplicate info, and duplicate contacts CSV."""
    reader = PdfReader(pdf_file)
    total_pages = len(reader.pages)
    
    # Phase 1: Extract info from all pages
    all_labels = []
    progress_bar = st.progress(0, text="Analyzing labels...")
    
    for i, page in enumerate(reader.pages):
        text = page.extract_text() or ''
        info = extract_label_info(text)
        info['page_index'] = i
        all_labels.append(info)
        progress_bar.progress((i + 1) / total_pages, text=f"Analyzing label {i+1}/{total_pages}")
    
    progress_bar.progress(1.0, text="Detecting duplicates...")
    
    # Phase 2: Detect duplicate orders (same order_id appearing multiple times)
    order_id_map = defaultdict(list)  # order_id -> list of label indices
    for idx, label in enumerate(all_labels):
        if label['order_id']:
            order_id_map[label['order_id']].append(idx)
    
    duplicate_order_ids = {oid: indices for oid, indices in order_id_map.items() if len(indices) > 1}
    
    # Phase 3: Detect duplicate contact numbers (same phone used by different orders)
    phone_map = defaultdict(list)  # phone -> list of label info dicts
    for label in all_labels:
        if label['phone']:
            phone_map[label['phone']].append(label)
    
    duplicate_phones = {phone: labels for phone, labels in phone_map.items() if len(labels) > 1}
    
    # Phase 4: Build filtered label list (remove duplicate orders if enabled)
    duplicate_page_indices = set()
    if filter_duplicates and duplicate_order_ids:
        for oid, indices in duplicate_order_ids.items():
            # Keep only the first occurrence, mark rest as duplicates
            for dup_idx in indices[1:]:
                duplicate_page_indices.add(all_labels[dup_idx]['page_index'])
    
    # Phase 5: Group pages by (date, courier, sku) — excluding duplicates
    groups = defaultdict(list)
    for label in all_labels:
        if label['page_index'] not in duplicate_page_indices:
            key = (label['date'], label['courier'], label['sku'])
            groups[key].append(label['page_index'])
    
    progress_bar.progress(1.0, text="Creating sorted PDFs...")
    
    # Phase 6: Create zip with sorted PDFs + duplicates PDF + duplicate contacts CSV
    zip_buffer = io.BytesIO()
    results = []
    
    with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zf:
        # Sorted label PDFs
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
                'file': filename,
                'date': date,
                'courier': courier,
                'sku': sku,
                'labels': len(page_indices)
            })
        
        # Duplicate orders PDF (if any duplicates were removed)
        if duplicate_page_indices:
            dup_writer = PdfWriter()
            for page_idx in sorted(duplicate_page_indices):
                dup_writer.add_page(reader.pages[page_idx])
            
            dup_pdf_buffer = io.BytesIO()
            dup_writer.write(dup_pdf_buffer)
            dup_pdf_buffer.seek(0)
            zf.writestr("_DUPLICATE_ORDERS.pdf", dup_pdf_buffer.getvalue())
        
        # Duplicate contacts CSV
        if duplicate_phones:
            csv_buffer = io.StringIO()
            writer_csv = csv.writer(csv_buffer)
            writer_csv.writerow(['Phone Number', 'Occurrences', 'Order IDs', 'Customer Names', 'SKUs', 'Couriers'])
            
            for phone, labels in sorted(duplicate_phones.items(), key=lambda x: len(x[1]), reverse=True):
                order_ids = ', '.join(filter(None, [l.get('order_id', '') for l in labels]))
                names = ', '.join(filter(None, [l.get('customer_name', '') for l in labels]))
                skus = ', '.join(set(filter(None, [l.get('sku', '') for l in labels])))
                couriers = ', '.join(set(filter(None, [l.get('courier', '') for l in labels])))
                writer_csv.writerow([phone, len(labels), order_ids, names, skus, couriers])
            
            zf.writestr("_DUPLICATE_CONTACTS.csv", csv_buffer.getvalue())
    
    zip_buffer.seek(0)
    progress_bar.empty()
    
    duplicate_info = {
        'duplicate_order_count': len(duplicate_order_ids),
        'duplicate_labels_removed': len(duplicate_page_indices),
        'duplicate_phone_count': len(duplicate_phones),
        'duplicate_order_ids': duplicate_order_ids,
        'duplicate_phones': duplicate_phones,
    }
    
    return zip_buffer, results, total_pages, duplicate_info


# --- UI ---

st.title("📦 Shiprocket Label Sorter 2.0")
st.markdown("**Sort bulk labels by Courier + SKU | Duplicate Order & Contact Detection**")

st.divider()

uploaded_file = st.file_uploader(
    "Upload your bulk labels PDF",
    type=['pdf'],
    help="Download bulk labels from Shiprocket and upload here"
)

if uploaded_file:
    st.info(f"📄 **{uploaded_file.name}** ({uploaded_file.size / 1024:.1f} KB)")
    
    filter_dupes = st.checkbox("🔍 Filter out duplicate orders", value=True, 
                                help="Removes duplicate order labels and keeps only the first occurrence")
    
    if st.button("🚀 Sort Labels", type="primary", use_container_width=True):
        with st.spinner("Processing..."):
            try:
                zip_buffer, results, total_pages, dup_info = sort_labels(uploaded_file, filter_duplicates=filter_dupes)
                
                st.success(f"✅ Sorted **{total_pages} labels** into **{len(results)} files**")
                
                # Duplicate Orders Summary
                if dup_info['duplicate_order_count'] > 0:
                    st.warning(
                        f"⚠️ Found **{dup_info['duplicate_order_count']} duplicate order(s)** "
                        f"({dup_info['duplicate_labels_removed']} extra labels {'removed' if filter_dupes else 'detected'})"
                    )
                    with st.expander("🔁 Duplicate Order Details"):
                        for oid, indices in dup_info['duplicate_order_ids'].items():
                            st.markdown(f"- **Order {oid}**: appears {len(indices)} times (pages {', '.join(str(i+1) for i in indices)})")
                        if filter_dupes:
                            st.caption("Duplicate labels saved in `_DUPLICATE_ORDERS.pdf` inside the ZIP.")
                
                # Duplicate Contacts Summary
                if dup_info['duplicate_phone_count'] > 0:
                    st.warning(
                        f"📱 Found **{dup_info['duplicate_phone_count']} duplicate contact number(s)**"
                    )
                    with st.expander("📱 Duplicate Contact Details"):
                        for phone, labels in sorted(dup_info['duplicate_phones'].items(), key=lambda x: len(x[1]), reverse=True):
                            order_ids = ', '.join(filter(None, [l.get('order_id', 'N/A') for l in labels]))
                            st.markdown(f"- **{phone}**: {len(labels)} orders ({order_ids})")
                        st.caption("Full list saved in `_DUPLICATE_CONTACTS.csv` inside the ZIP.")
                
                # Results table
                st.subheader("📊 Summary")
                
                for r in results:
                    col1, col2, col3 = st.columns([2, 1, 1])
                    with col1:
                        st.markdown(f"**{r['courier']}** / {r['sku']}")
                    with col2:
                        st.markdown(f"📅 {r['date']}")
                    with col3:
                        st.markdown(f"🏷️ {r['labels']} labels")
                
                st.divider()
                
                # Download button
                st.download_button(
                    label="📥 Download All (ZIP)",
                    data=zip_buffer,
                    file_name=f"sorted_labels_{datetime.now().strftime('%Y%m%d_%H%M%S')}.zip",
                    mime="application/zip",
                    type="primary",
                    use_container_width=True
                )
                
            except Exception as e:
                st.error(f"❌ Error: {str(e)}")

st.divider()

with st.expander("ℹ️ How it works"):
    st.markdown("""
    1. **Upload** your bulk labels PDF from Shiprocket
    2. The tool scans each label and extracts:
       - Courier name (Ekart, Delhivery, Xpressbees, etc.)
       - SKU code
       - Invoice date
       - Order ID & Contact number
    3. **Duplicate Detection:**
       - Finds duplicate orders (same Order ID appearing multiple times)
       - Finds duplicate contact numbers (same phone across different orders)
       - Optionally removes duplicate order labels from output
    4. Labels are grouped and saved as separate PDFs
    5. **Download** the ZIP with all sorted files
    
    **Output format:** `YYYY-MM-DD_Courier_SKU.pdf`
    
    **Extra files in ZIP:**
    - `_DUPLICATE_ORDERS.pdf` — Removed duplicate order labels
    - `_DUPLICATE_CONTACTS.csv` — List of phone numbers used in multiple orders
    
    **Supported Couriers:** Ekart, Delhivery, Xpressbees, BlueDart, DTDC, Shadowfax, Ecom Express
    """)

st.caption("Built with ❤️ by JSK Labs")
