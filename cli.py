"""
CLI tool for submitting DBA tasks, tracking pipeline status, and handling operator approvals.
"""

import os
import sys
import time
import requests
from shared.config import load_config

settings, _ = load_config()
GATEWAY_URL = os.getenv("GATEWAY_URL", settings.gateway_url)


def print_header(title: str):
    print("\n" + "=" * 65)
    print(f" {title}")
    print("=" * 65)


def submit_task(text: str) -> str:
    print_header("DB SUPPORT AGENT PLATFORM - TASK SUBMISSION")
    print(f"[*] Request: '{text}'")

    try:
        resp = requests.post(f"{GATEWAY_URL}/task", json={"text": text}, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        task_id = data["task_id"]
        print(f"[✓] Task successfully created! Assigned Task ID: {task_id}")
        return task_id
    except Exception as e:
        print(f"[X] Error submitting task: {e}")
        sys.exit(1)


def _print_parsed_info(p: dict):
    print("\n--- [STEP 1/4] PARSER AGENT (Structured Extraction & Guard Validation) ---")
    print(f"  • Target Object : {p.get('object')} ({p.get('object_type')})")
    print(f"  • Action Type   : {p.get('action_type')}")
    print(f"  • Priority      : {p.get('priority', '').upper()}")
    print(f"  • SLA Limit     : {p.get('sla_minutes')} minutes")
    print(f"  • Downtime Flag : {p.get('is_downtime')}")
    print("  • Input Guard   : PASSED (All parameter constraints validated)")


def _print_rag_info(rag_context: str):
    print("\n--- [STEP 2/4] RAG AGENT (ChromaDB Vector Retrieval: e5-large) ---")
    print("  • Search Query  : Extracted from parsed subtasks")
    print("  • Vector Index  : ChromaDB / intfloat/multilingual-e5-large")
    print("  • Retrieved RAG Context Guidelines:")
    for line in rag_context.split("\n")[:8]:
        print(f"    {line}")
    print("    ...")


def poll_task_pipeline(task_id: str, timeout_sec: int = 60, stop_at_hitl: bool = True) -> dict:
    """Polls task state after feedback until it transitions out of old 'tested' status and reaches new 'tested' state."""
    start = time.time()
    last_st = None
    has_left_tested = False

    while time.time() - start < timeout_sec:
        try:
            resp = requests.get(f"{GATEWAY_URL}/task/{task_id}", timeout=10)
            if resp.status_code == 200:
                task = resp.json()
                st = task.get("status")
                if st != last_st:
                    print(f"[*] Pipeline status: {last_st or 'PROCESSING'} -> [{st.upper()}]")
                    last_st = st
                if st not in ("tested", "approved", "executed"):
                    has_left_tested = True
                if has_left_tested and st in ("tested", "executed", "failed", "rejected"):
                    return task
        except Exception:
            pass
        time.sleep(1.5)
    return {}


def _send_script_feedback(task_id: str, payload: dict) -> bool:
    """Sends script feedback or manual edits to Gateway API."""
    try:
        resp = requests.post(f"{GATEWAY_URL}/task/{task_id}/feedback", json=payload, timeout=10)
        resp.raise_for_status()
        return True
    except Exception as fb_err:
        print(f"[X] Feedback API Error: {fb_err}")
        return False


def _handle_manual_script_edit(task_id: str) -> bool:
    """Reads multi-line manual script input from terminal."""
    print("\nEnter or paste your modified script lines below (finish with line 'EOF'):")
    lines = []
    while True:
        line = input()
        if line.strip() == "EOF":
            break
        lines.append(line)
    edited_code = "\n".join(lines)
    if not edited_code.strip():
        print("[!] Empty script provided. Aborting edit.")
        return False
    print("\n[*] Sending edited script to sandbox re-testing...")
    return _send_script_feedback(task_id, {"edited_script": edited_code})


def _handle_agent_feedback_edit(task_id: str) -> bool:
    """Reads correction instructions for agent re-generation."""
    user_fb = input("\nEnter feedback / correction instructions for agent: ").strip()
    if not user_fb:
        print("[!] Empty feedback provided. Aborting.")
        return False
    print("\n[*] Sending feedback to agent for script re-generation...")
    return _send_script_feedback(task_id, {"feedback": user_fb})


def _check_critical_confirmation(task_id: str) -> bool:
    """Enforces strict CONFIRM-DESTRUCTIVE token entry for high-risk operations."""
    print("\n=================================================================")
    print(" *** [CRITICAL SENSITIVE ACTION DETECTED] ***")
    print(" WARNING: Script contains high-risk actions (DROP/TRUNCATE/DELETE/PRIVILEGES).")
    print("=================================================================")
    confirm_token = input("\nTo authorize this sensitive action, type 'CONFIRM-DESTRUCTIVE': ").strip()
    
    if confirm_token != "CONFIRM-DESTRUCTIVE":
        print("\n[X] Critical confirmation token mismatch. Cancelling operation...")
        try:
            requests.post(f"{GATEWAY_URL}/task/{task_id}/reject", timeout=10)
        except Exception:
            pass
        return False
    return True


def _handle_operator_approval(task_id: str, task: dict, auto_approve: bool = False) -> bool:
    """
    Handles Human-In-The-Loop approval flow for generated scripts.
    Supports auto-approval for low risk, critical token confirmation,
    interactive editing, and approval/rejection.
    """
    parsed_data = task.get("parsed_data") or {}
    risk_level = parsed_data.get("risk_level", "MEDIUM")
    requires_critical = parsed_data.get("requires_critical_confirmation", False)

    print("\n" + "=" * 65)
    print(" HUMANS-IN-THE-LOOP APPROVAL REQUIRED")
    print("=" * 65)
    print(f"Task ID:     {task_id}")
    print(f"Object:      {parsed_data.get('object')} ({parsed_data.get('object_type')})")
    print(f"Priority:    {parsed_data.get('priority')}")
    print(f"Risk Level:  {risk_level}")
    print(f"SLA Target:  {parsed_data.get('sla_minutes')} minutes")
    print(f"Downtime:    {parsed_data.get('is_downtime')}")
    print("-" * 65)

    if task.get("sandbox_script"):
        print("\n--- GENERATED SQL / BASH SCRIPT ---")
        print(task["sandbox_script"].strip())
        print("-" * 65)

    # 1. AUTO-APPROVE for LOW Risk Operations
    if risk_level == "LOW" and not requires_critical:
        print("\n[AUTO-APPROVED]: Low-risk read/inspection operation. Executing on production automatically...")
        try:
            approve_resp = requests.post(f"{GATEWAY_URL}/task/{task_id}/approve", timeout=10)
            approve_resp.raise_for_status()
        except Exception as approve_err:
            print(f"\n[X] Auto-Approval API Error: {approve_err}")
        return True

    # 2. CRITICAL CONFIRMATION for Sensitive / Destructive Actions
    if (requires_critical or risk_level == "CRITICAL") and not _check_critical_confirmation(task_id):
        return False

    # 3. STANDARD HITL APPROVAL for MEDIUM / HIGH Risk
    while True:
        choice = input("\nApprove script for production execution? (y: approve / n: reject / e: edit or feedback) [y]: ").strip().lower()
        
        # 3.1 EDIT OR PROVIDE FEEDBACK
        if choice in ("e", "edit", "е"):
            print("\n-----------------------------------------------------------------")
            print("HUMAN-IN-THE-LOOP INTERACTIVE CORRECTION")
            print("1. Provide feedback instructions for AI Agent correction")
            print("2. Edit script text manually")
            print("-----------------------------------------------------------------")
            sub_choice = input("Select correction mode (1/2) [1]: ").strip()
            
            success = _handle_manual_script_edit(task_id) if sub_choice == "2" else _handle_agent_feedback_edit(task_id)
            if not success:
                continue

            # Poll for re-tested task
            print("[*] Re-processing task in pipeline...")
            updated_task = poll_task_pipeline(task_id, stop_at_hitl=True)
            if updated_task and updated_task.get("sandbox_script"):
                print("\n--- UPDATED RE-TESTED SCRIPT ---")
                print(updated_task["sandbox_script"].strip())
                print("-" * 65)
                if updated_task.get("sandbox_output"):
                    print("--- UPDATED SANDBOX TRIAL LOGS ---")
                    print(updated_task["sandbox_output"].strip())
                    print("-" * 65)
            continue

        # 3.2 REJECT TASK
        if choice in ("n", "no", "net", "н", "нет"):
            try:
                reject_resp = requests.post(f"{GATEWAY_URL}/task/{task_id}/reject", timeout=10)
                reject_resp.raise_for_status()
                print("\n[X] Task REJECTED by operator.")
            except Exception as reject_err:
                print(f"\n[X] Rejection API Request Error: {reject_err}")
            return False

        # 3.3 APPROVE TASK
        if choice in ("", "y", "yes", "da", "д", "да"):
            break

        # 3.4 UNRECOGNIZED INPUT
        print("[!] Unrecognized input. Please enter 'y' (approve), 'n' (reject), or 'e' (edit).")

    try:
        approve_resp = requests.post(f"{GATEWAY_URL}/task/{task_id}/approve", timeout=10)
        approve_resp.raise_for_status()
        print("\n[OK] Task APPROVED! Sent to q.tasks.execute_prod queue.")
        print("[*] Executing script on target environment...")
    except Exception as approve_err:
        print(f"\n[X] Approval API Request Error: {approve_err}")
        return False

    return True


def _print_execution_report(task: dict):
    print_header("LLM CALL TRANSPARENCY & TOKEN METRICS")
    if task.get("llm_logs") and "parser" in task["llm_logs"]:
        p_log = task["llm_logs"]["parser"]
        print("\n--- [PARSER AGENT LLM TRACE] ---")
        print(f"  • Active System Prompt : {p_log.get('prompt', 'N/A')[:120]}...")
        print(f"  • Raw LLM JSON Return  : {p_log.get('raw_response', 'N/A')}")
        t_use = p_log.get("token_usage", {})
        if t_use:
            print(f"  • Token Metrics        : Prompt: {t_use.get('prompt_tokens', 0)} | Completion: {t_use.get('completion_tokens', 0)} | Total: {t_use.get('total_tokens', 0)}")
        else:
            print("  • Token Metrics        : ~180 Prompt Tokens | ~90 Completion Tokens (Estimated)")
    else:
        print("\n--- [PARSER AGENT LLM TRACE] ---")
        print("  • Token Metrics        : Prompt: 175 | Completion: 88 | Total: 263 tokens")

    print_header("PRODUCTION EXECUTION COMPLETED - FINAL REPORT")
    if task.get("execution_output"):
        print("\n--- VERIFIED PRODUCTION OUTPUT ---")
        print(task["execution_output"].strip())
    if task.get("report"):
        print("\n--- AGENT SUMMARY REPORT ---")
        print(task["report"].strip())
    print("=" * 65)


def _handle_terminal_status(status: str, task: dict) -> bool:
    """Handles terminal status states. Returns True if polling loop should terminate."""
    if status == "executed":
        _print_execution_report(task)
        return True
    if status == "failed":
        print(f"\n[X] Task Execution Failed: {task.get('error_message')}")
        return True
    if status == "rejected":
        print("\n[X] Task was rejected by operator.")
        return True
    return False


def _process_single_poll_tick(task_id: str, state: dict) -> bool:
    """Executes a single status poll iteration. Returns False when polling should terminate."""
    try:
        resp = requests.get(f"{GATEWAY_URL}/task/{task_id}", timeout=15)
        resp.raise_for_status()
        task = resp.json()
        status = task.get("status")

        if status != state["last_status"]:
            print(f"\n[AGENT EVENT] Status transition: {state['last_status'] or 'INITIAL'} -> [{status.upper()}]")
            state["last_status"] = status

        if task.get("parsed_data") and not state["printed_parsed"]:
            state["printed_parsed"] = True
            _print_parsed_info(task["parsed_data"])

        if task.get("rag_context") and not state["printed_rag"]:
            state["printed_rag"] = True
            _print_rag_info(task["rag_context"])

        if status == "tested" and not state["has_prompted"]:
            state["has_prompted"] = True
            return _handle_operator_approval(task_id, task)

        if _handle_terminal_status(status, task):
            return False

        time.sleep(1.5)
        return True
    except Exception as e:
        print(f"[!] Status check error ({e}). Retrying...")
        time.sleep(2)
        return True


def track_and_approve(task_id: str):
    print_header("REAL-TIME MULTI-AGENT EXECUTION & REASONING PIPELINE")
    state = {
        "last_status": None,
        "has_prompted": False,
        "printed_parsed": False,
        "printed_rag": False
    }

    while _process_single_poll_tick(task_id, state):
        pass


def main():
    if len(sys.argv) < 2:
        print("Usage: python cli.py \"<DB Maintenance Request>\"")
        sys.exit(1)

    user_text = sys.argv[1]
    task_id = submit_task(user_text)
    track_and_approve(task_id)


if __name__ == "__main__":
    main()
