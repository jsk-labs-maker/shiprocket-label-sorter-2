#!/usr/bin/env python3
"""
Shiprocket Label Sorter 2.0
============================
Sorts bulk shipping labels by Courier and SKU, outputs organized PDFs.
Duplicate order detection by Customer Contact Number.

Output format: YYYY-MM-DD_Courier_SKU.pdf

Author: Kluzo 😎 for Dhruv Shetty / JSK Labs
"""

import re
import os
import sys
import csv
from pathlib import Path
from collections import defaultdict
from datetime import datetime

from pypdf import PdfReader, PdfWriter


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
    
    # --- PHONE (10-digit in Ship To block) ---
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


def sort_labels(input_pdf: str, output_dir: str = None, filter_duplicates: bool = True) -> dict:
    """
    Sort labels from input PDF into separate PDFs by Courier + SKU.
    Detects duplicate orders by customer contact number.
    """
    input_path = Path(input_pdf)
    
    if not input_path.exists():
        raise FileNotFoundError(f"Input PDF not found: {input_pdf}")
    
    if output_dir is None:
        output_dir = input_path.parent / 'sorted_labels'
    else:
        output_dir = Path(output_dir)
    
    output_dir.mkdir(parents=True, exist_ok=True)
    
    print(f"📄 Reading: {input_path.name}")
    reader = PdfReader(str(input_path))
    total_pages = len(reader.pages)
    print(f"   Found {total_pages} labels")
    
    # Phase 1: Extract info from all pages
    all_labels = []
    for i, page in enumerate(reader.pages):
        text = page.extract_text() or ''
        info = extract_label_info(text)
        info['page_index'] = i
        all_labels.append(info)
        
        if (i + 1) % 50 == 0:
            print(f"   Processed {i + 1}/{total_pages} labels...")
    
    # Phase 2: Detect duplicates by phone number
    phone_order_map = defaultdict(list)
    for idx, label in enumerate(all_labels):
        if label['phone']:
            phone_order_map[label['phone']].append(idx)
    
    duplicate_phones = {phone: indices for phone, indices in phone_order_map.items() if len(indices) > 1}
    
    if duplicate_phones:
        print(f"\n📱 Found {len(duplicate_phones)} duplicate contact number(s):")
        for phone, indices in duplicate_phones.items():
            labels = [all_labels[i] for i in indices]
            order_ids = ', '.join(filter(None, [l.get('order_id', 'N/A') for l in labels]))
            names = ', '.join(filter(None, [l.get('customer_name', 'N/A') for l in labels]))
            print(f"   {phone}: {len(indices)} orders ({order_ids}) — {names}")
    
    # Phase 3: Build filtered set
    duplicate_page_indices = set()
    if filter_duplicates and duplicate_phones:
        for phone, indices in duplicate_phones.items():
            for dup_idx in indices[1:]:
                duplicate_page_indices.add(all_labels[dup_idx]['page_index'])
        print(f"\n🗑️  Removing {len(duplicate_page_indices)} duplicate label(s) from output")
    
    # Phase 4: Group by (date, courier, sku)
    groups = defaultdict(list)
    for label in all_labels:
        if label['page_index'] not in duplicate_page_indices:
            key = (label['date'], label['courier'], label['sku'])
            groups[key].append(label['page_index'])
    
    print(f"\n📦 Found {len(groups)} unique groups")
    
    # Phase 5: Create output PDFs
    results = []
    for (date, courier, sku), page_indices in sorted(groups.items()):
        filename = f"{date}_{courier}_{sku}.pdf"
        output_path = output_dir / filename
        
        writer = PdfWriter()
        for idx in page_indices:
            writer.add_page(reader.pages[idx])
        
        with open(output_path, 'wb') as f:
            writer.write(f)
        
        results.append({
            'file': filename, 'date': date, 'courier': courier,
            'sku': sku, 'labels': len(page_indices)
        })
        print(f"   ✅ {filename} ({len(page_indices)} labels)")
    
    # Phase 6: Save duplicate orders PDF
    if duplicate_page_indices:
        dup_writer = PdfWriter()
        for page_idx in sorted(duplicate_page_indices):
            dup_writer.add_page(reader.pages[page_idx])
        
        dup_path = output_dir / "_DUPLICATE_ORDERS.pdf"
        with open(dup_path, 'wb') as f:
            dup_writer.write(f)
        print(f"   🔁 _DUPLICATE_ORDERS.pdf ({len(duplicate_page_indices)} labels)")
    
    # Phase 7: Save duplicate contacts CSV
    if duplicate_phones:
        csv_path = output_dir / "_DUPLICATE_CONTACTS.csv"
        with open(csv_path, 'w', newline='') as f:
            writer_csv = csv.writer(f)
            writer_csv.writerow(['Phone Number', 'Occurrences', 'Order IDs', 'Customer Names', 'SKUs', 'Couriers'])
            
            for phone, indices in sorted(duplicate_phones.items(), key=lambda x: len(x[1]), reverse=True):
                labels = [all_labels[i] for i in indices]
                order_ids = ', '.join(filter(None, [l.get('order_id', '') for l in labels]))
                names = ', '.join(filter(None, [l.get('customer_name', '') for l in labels]))
                skus = ', '.join(set(filter(None, [l.get('sku', '') for l in labels])))
                couriers = ', '.join(set(filter(None, [l.get('courier', '') for l in labels])))
                writer_csv.writerow([phone, len(indices), order_ids, names, skus, couriers])
        
        print(f"   📱 _DUPLICATE_CONTACTS.csv ({len(duplicate_phones)} numbers)")
    
    print(f"\n🎉 Done! {len(results)} files created in: {output_dir}")
    
    return {
        'input': str(input_path),
        'output_dir': str(output_dir),
        'total_labels': total_pages,
        'files': results,
        'duplicates': {
            'duplicate_phone_count': len(duplicate_phones),
            'duplicate_labels_removed': len(duplicate_page_indices),
            'duplicate_phones': {phone: len(indices) for phone, indices in duplicate_phones.items()},
        }
    }


def main():
    """CLI entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Sort Shiprocket shipping labels by Courier and SKU'
    )
    parser.add_argument('input_pdf', help='Path to input PDF with bulk labels')
    parser.add_argument('-o', '--output', help='Output directory (default: ./sorted_labels)')
    parser.add_argument('--no-filter-duplicates', action='store_true',
                        help='Keep duplicate order labels in output (still reports them)')
    
    args = parser.parse_args()
    
    try:
        result = sort_labels(args.input_pdf, args.output, 
                            filter_duplicates=not args.no_filter_duplicates)
        
        print("\n" + "="*50)
        print("SUMMARY")
        print("="*50)
        print(f"Total labels processed: {result['total_labels']}")
        print(f"Output files created: {len(result['files'])}")
        print(f"Output directory: {result['output_dir']}")
        
        dup = result['duplicates']
        if dup['duplicate_phone_count'] > 0:
            print(f"Duplicate contact numbers: {dup['duplicate_phone_count']}")
            print(f"Duplicate labels removed: {dup['duplicate_labels_removed']}")
            print(f"  → See _DUPLICATE_CONTACTS.csv")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()
