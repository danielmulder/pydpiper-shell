# src/auditor/dom/qngine.py
from typing import List, Dict, Any

from .models import HTMLDocument
from .core import ElementBase
from .registry import DOMRegistry


class QNGINE:
    """
    Quality Engine (QNGINE) for auditing HTML Documents.

    It traverses the DOM tree constructed by the DOMBuilder and applies
    registered audit rules to every node. It also performs global document-level checks.
    """

    def __init__(self):
        """Initializes the engine by discovering and loading all available audit rules."""
        DOMRegistry.discover()
        self.rules = DOMRegistry.get_all_rules()

    def run_audit(self, doc: HTMLDocument) -> List[Dict[str, Any]]:
        """
        Runs the full audit suite on a parsed HTMLDocument.

        Args:
            doc (HTMLDocument): The parsed document model containing head and body.

        Returns:
            List[Dict[str, Any]]: A list of findings/issues detected during the audit.
        """
        findings = []

        # --- Root Level Checks ---
        if "missing_doctype" in doc.doc_errors:
            findings.append({
                "code": "MISSING",
                "msg": "Document missing <!DOCTYPE html>",
                "el": "doctype",
                "sev": "CRITICAL",
                "cat": "HTML"
            })
        if "missing_html_root_tag" in doc.doc_errors:
            findings.append({
                "code": "MISSING_ROOT",
                "msg": "Document missing <html> root",
                "el": "html",
                "sev": "CRITICAL",
                "cat": "HTML"
            })

        # --- State Tracking for Global Checks ---
        found_h1 = False

        def traverse(node: ElementBase):
            nonlocal found_h1
            if not node:
                return

            # Track global state: Check if we encounter an H1 tag
            if node.tag == 'h1':
                found_h1 = True

            # Apply all registered rules to the current node
            for rule in self.rules:
                # Rule is expected to return a List of tuples: [(Code, Msg, ElType, Sev, Cat)]
                # If rule does not apply (returns empty string/None), this loop is skipped.
                results = rule(node)
                if not results:
                    continue

                for (code, msg, el_type, sev, cat) in results:
                    findings.append({
                        "code": code,
                        "msg": msg,
                        "el": el_type,
                        "sev": sev,
                        "cat": cat
                    })

            # Recursively traverse children
            for child in node.children:
                traverse(child)

        # Start traversal on Head and Body
        traverse(doc.head)
        traverse(doc.body)

        # --- Post-Traversal Checks ---

        # Check: Missing H1 (Critical for SEO)
        if doc.body and not found_h1:
            findings.append({
                "code": "MISSING_H1",
                "msg": "Document does not contain an <h1> tag",
                "el": "h1",
                "sev": "CRITICAL",
                "cat": "HEADINGS"
            })

        return findings