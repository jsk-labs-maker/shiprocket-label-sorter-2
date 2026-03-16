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
from collections import defaultdict
from datetime import datetime
from pypdf import PdfReader, PdfWriter

st.set_page_config(
    page_title="Label Sorter 2.0 | JSK Labs",
    page_icon="📦",
    layout="centered"
)

# --- Helper Functions ---

def normalize_sku(sku_raw: str) -> str:
    """Normalize SKU for filename safety."""
    return re.sub(r'[^\w\-]', '', sku_raw.replace(' ', '-'))[:50]


def extract_label_info(page_text: str) -> dict:
    """
    Extract courier, SKU, date, order ID, and phone from Shiprocket label text.
    
    Tested against actual Shiprocket label PDF text format:
      - Courier line: "Ekart Special Surface 500gm" / "Delhivery DS 500gm" / "Shadowfax Surface"
      - SKU in table: between header "Item\\nSKU\\nQty\\nPrice\\nTotal\\n" and "\\n<digit>\\n₹"
      - Date: "Invoice Date: DD/MM/YYYY"
      - Order: "Order#: <number>"
      - Phone: 10-digit number in Ship To block (before "Dimensions:")
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
    # Actual format: "Item\nSKU\nQty\nPrice\nTotal\n<item desc lines>\n<SKU lines>\n<qty>\n₹"
    sku_block = re.search(r'Item\nSKU\nQty\nPrice\nTotal\n(.+?)\n(\d+)\n₹', page_text, re.DOTALL)
    if sku_block:
        block_lines = sku_block.group(1).strip().split('\n')
        # SKU is the last short line(s) before qty
        # Item description lines are long (>40 chars) or contain "..."
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
        # Fallback: YYYY-MM-DD
        date_match = re.search(r'Invoice Date:\s*(\d{4}-\d{2}-\d{2})', page_text)
        if date_match:
            info['date'] = date_match.group(1)
    
    # --- ORDER ID (Order#: <number>) ---
    order_match = re.search(r'Order#:\s*(\d+)', page_text)
    if order_match:
        info['order_id'] = order_match.group(1).strip()
    
    # --- PHONE (10-digit in Ship To block, before "Dimensions:") ---
    ship_to_block = re.search(r'Ship To\n(.+?)Dimensions:', page_text, re.DOTALL)
    if ship_to_block:
        phone_match = re.search(r'\b(\d{10})\b', ship_to_block.group(1))
        if phone_match:
            phone = phone_match.group(1)
            if phone[0] in '6789':
                info['phone'] = phone
    
    # --- CUSTOMER NAME (line after "Ship To\n") ---
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
    
    # Phase 1: Extract info from all pages
    all_labels = []
    progress_bar = st.progress(0, text="Analyzing labels...")
    
    for i, page in enumerate(reader.pages):
        text = page.extract_text() or ''
        info = extract_label_info(text)
        info['page_index'] = i
        all_labels.append(info)
        progress_bar.progress((i + 1) / total_pages, text=f"Analyzing label {i+1}/{total_pages}")
    
    progress_bar.progress(1.0, text="Detecting duplicates by contact number...")
    
    # Phase 2: Detect duplicate orders by PHONE NUMBER
    phone_order_map = defaultdict(list)
    for idx, label in enumerate(all_labels):
        if label['phone']:
            phone_order_map[label['phone']].append(idx)
    
    duplicate_phones = {phone: indices for phone, indices in phone_order_map.items() if len(indices) > 1}
    
    # Phase 3: Build filtered list (remove duplicate phone orders if enabled)
    duplicate_page_indices = set()
    if filter_duplicates and duplicate_phones:
        for phone, indices in duplicate_phones.items():
            for dup_idx in indices[1:]:
                duplicate_page_indices.add(all_labels[dup_idx]['page_index'])
    
    # Phase 4: Group pages by (date, courier, sku) — excluding duplicates
    groups = defaultdict(list)
    for label in all_labels:
        if label['page_index'] not in duplicate_page_indices:
            key = (label['date'], label['courier'], label['sku'])
            groups[key].append(label['page_index'])
    
    progress_bar.progress(1.0, text="Creating sorted PDFs...")
    
    # Phase 5: Create zip
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
        
        # Duplicate orders PDF
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
    
    duplicate_info = {
        'duplicate_phone_count': len(duplicate_phones),
        'duplicate_labels_removed': len(duplicate_page_indices),
        'duplicate_phones': duplicate_phones,
        'all_labels': all_labels,
    }
    
    return zip_buffer, results, total_pages, duplicate_info


# --- UI ---

st.title("📦 Shiprocket Label Sorter 2.0")
st.markdown("**Sort bulk labels by Courier + SKU | Duplicate Detection by Contact No.**")

st.divider()

uploaded_file = st.file_uploader(
    "Upload your bulk labels PDF",
    type=['pdf'],
    help="Download bulk labels from Shiprocket and upload here"
)

if uploaded_file:
    st.info(f"📄 **{uploaded_file.name}** ({uploaded_file.size / 1024:.1f} KB)")
    
    filter_dupes = st.checkbox("🔍 Filter out duplicate orders (by contact number)", value=True, 
                                help="If the same phone number appears in multiple orders, keeps only the first and removes the rest")
    
    if st.button("🚀 Sort Labels", type="primary", use_container_width=True):
        with st.spinner("Processing..."):
            try:
                zip_buffer, results, total_pages, dup_info = sort_labels(uploaded_file, filter_duplicates=filter_dupes)
                
                st.success(f"✅ Sorted **{total_pages} labels** into **{len(results)} files**")
                
                # Duplicate Contacts Summary
                if dup_info['duplicate_phone_count'] > 0:
                    st.warning(
                        f"📱 Found **{dup_info['duplicate_phone_count']} duplicate contact number(s)** "
                        f"({dup_info['duplicate_labels_removed']} extra labels {'removed' if filter_dupes else 'detected'})"
                    )
                    with st.expander("📱 Duplicate Contact Details"):
                        all_labels = dup_info['all_labels']
                        for phone, indices in sorted(dup_info['duplicate_phones'].items(), key=lambda x: len(x[1]), reverse=True):
                            labels = [all_labels[i] for i in indices]
                            order_ids = ', '.join(filter(None, [l.get('order_id', 'N/A') for l in labels]))
                            names = ', '.join(filter(None, [l.get('customer_name', 'N/A') for l in labels]))
                            st.markdown(f"- **{phone}** — {len(indices)} orders — Orders: {order_ids} — Names: {names}")
                        st.caption("Full list saved in `_DUPLICATE_CONTACTS.csv` inside the ZIP.")
                        if filter_dupes:
                            st.caption("Removed duplicate labels saved in `_DUPLICATE_ORDERS.pdf` inside the ZIP.")
                else:
                    st.info("✅ No duplicate contact numbers found")
                
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
       - Courier name (Ekart, Delhivery, Xpressbees, Shadowfax, etc.)
       - SKU code (from label table)
       - Invoice date
       - Order ID & Contact number
    3. **Duplicate Detection (by Contact Number):**
       - Finds orders with the **same phone number**
       - Optionally removes duplicate labels from sorted output
       - Generates `_DUPLICATE_CONTACTS.csv` with full details
    4. Labels are grouped and saved as separate PDFs
    5. **Download** the ZIP with all sorted files
    
    **Output format:** `YYYY-MM-DD_Courier_SKU.pdf`
    
    **Extra files in ZIP:**
    - `_DUPLICATE_ORDERS.pdf` — Removed duplicate order labels  
    - `_DUPLICATE_CONTACTS.csv` — Phone numbers appearing in multiple orders
    
    **Supported Couriers:** Ekart, Delhivery, Xpressbees, BlueDart, DTDC, Shadowfax, Ecom Express
    """)

st.caption("Built with ❤️ by JSK Labs")
