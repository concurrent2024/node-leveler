"""Alert fan-out: one throttle per channel, and mail problems never stop a round."""
import pytest

import balancer as B


class FakeSMTP:
    instances = []

    def __init__(self, host, port, timeout=None):
        self.host, self.port = host, port
        self.messages = []
        self.logged_in = None
        self.started_tls = False
        FakeSMTP.instances.append(self)

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def starttls(self):
        self.started_tls = True

    def login(self, user, password):
        self.logged_in = (user, password)

    def send_message(self, msg):
        self.messages.append(msg)


@pytest.fixture(autouse=True)
def _fresh_instances():
    FakeSMTP.instances.clear()
    yield
    FakeSMTP.instances.clear()


def _cfg(**kw):
    base = {"notify_enabled": True, "dingtalk_token": "", "dingtalk_secret": "",
            "notify_email_enabled": True, "smtp_host": "smtp.example", "smtp_port": 465,
            "smtp_security": "ssl", "smtp_from": "bot@example", "smtp_to": ["ops@example"],
            "smtp_user": "bot@example", "smtp_pass": "secret", "smtp_subject_prefix": "[leveler]",
            "cluster_name": "prod-x", "notify_title": "node-leveler", "notify_min_interval_min": 180,
            "mode": "live", "cfg_cm": ("cluster-ops", "node-leveler-config")}
    base.update(kw)
    for k, v in base.items():
        B.CFG[k] = v


def _state():
    return {"notify": {}}


def test_email_is_built_with_cluster_and_reason(monkeypatch):
    _cfg()
    monkeypatch.setattr(B.smtplib, "SMTP_SSL", FakeSMTP)
    state = _state()

    B.notify(state, "pool-slot-high", ["Pool slot usage above 85%", "",
                                      "  common/4C   136/138   98.6%   2 free"])

    msg = FakeSMTP.instances[-1].messages[-1]
    assert msg["Subject"] == "[leveler][prod-x][WARN] pool-slot-high"
    assert msg["To"] == "ops@example" and msg["From"] == "bot@example"
    body = msg.get_content()
    assert "pool-slot-high" in body and "common/4C" in body
    assert "kubectl -n cluster-ops get cj node-leveler" in body
    assert "**" not in body and "`" not in body          # Markdown never reaches the mail
    assert state["notify"]["pool-slot-high:email"] > 0


def test_starttls_is_used_when_requested(monkeypatch):
    _cfg(smtp_security="starttls", smtp_port=587)
    smtp = FakeSMTP
    monkeypatch.setattr(B.smtplib, "SMTP", smtp)
    monkeypatch.setattr(smtp, "_dummy", 1, raising=False)

    B.notify(_state(), "node-hot", ["node x"])
    assert smtp.instances[-1].started_tls is True


def test_missing_password_skips_without_raising(monkeypatch):
    _cfg(smtp_pass="")
    monkeypatch.setattr(B.smtplib, "SMTP_SSL", FakeSMTP)
    assert B.notify(_state(), "abort", ["x"]) is True
    assert FakeSMTP.instances == []          # no connection was even attempted


def test_smtp_failure_is_swallowed(monkeypatch):
    _cfg()

    class Boom(FakeSMTP):
        def login(self, user, password):
            raise ConnectionRefusedError("smtp down")

    monkeypatch.setattr(B.smtplib, "SMTP_SSL", Boom)
    assert B.notify(_state(), "abort", ["x"]) is True      # the round carries on


def test_throttle_is_per_channel(monkeypatch):
    _cfg()
    monkeypatch.setattr(B.smtplib, "SMTP_SSL", FakeSMTP)
    state = _state()

    B.notify(state, "pool-slot-high", ["first"])
    B.notify(state, "pool-slot-high", ["second"])
    assert len(FakeSMTP.instances) == 1                     # second was throttled

    B.notify(state, "pool-slot-high", ["forced"], force=True)
    assert len(FakeSMTP.instances) == 2                     # force bypasses the throttle


def test_a_missing_dingtalk_token_does_not_block_email(monkeypatch):
    _cfg(dingtalk_token="")
    monkeypatch.setattr(B.smtplib, "SMTP_SSL", FakeSMTP)
    B.notify(_state(), "pool-slot-high", ["x"])
    assert len(FakeSMTP.instances) == 1


def test_subject_carries_level_and_headline(monkeypatch):
    _cfg()
    monkeypatch.setattr(B.smtplib, "SMTP_SSL", FakeSMTP)

    B.notify(_state(), "abort", ["x"], headline="CrashLoopBackOff team-a/orders-api", force=True)
    assert FakeSMTP.instances[-1].messages[-1]["Subject"] == \
        "[leveler][prod-x][CRIT] abort CrashLoopBackOff team-a/orders-api"


def test_alert_levels():
    assert B.alert_level("abort") == "CRIT"
    assert B.alert_level("auto-downgrade") == "CRIT"
    assert B.alert_level("pool-slot") == "WARN"
    assert B.alert_level("node-hot") == "WARN"
    assert B.alert_level("something-else") == "INFO"


def test_table_is_aligned_and_plain():
    rows = B.fmt_table(["node", "slots"], [["n1", "46/46"], ["n2", "187/211"]], [12, 8])
    assert rows[0].startswith("  node")
    assert set(rows[1].strip()) == {"-"}
    assert "|" not in "".join(rows)
    assert all(not r.endswith(" ") for r in rows)


def test_header_has_cluster_mode_and_a_timestamp():
    _cfg()
    text = "\n".join(B.alert_header("pool-slot", "98.6% (common/4C)"))
    assert "node-leveler | prod-x | WARN pool-slot" in text
    assert "98.6% (common/4C)" in text
    assert "CST" in text and "mode=live" in text and "job=" in text


def test_chinese_template(monkeypatch):
    _cfg(notify_lang="zh")
    monkeypatch.setattr(B.smtplib, "SMTP_SSL", FakeSMTP)
    B.notify(_state(), "pool-slot", ["池槽位使用超过 85%", "", "  common/4C   136/138   98.6%"], headline="98.6% (common/4C)")
    body = FakeSMTP.instances[-1].messages[-1].get_content()
    assert "模式=live" in body and "自查命令" in body
    assert "Self-check" not in body


def test_language_falls_back_to_english(monkeypatch):
    _cfg(notify_lang="fr")
    monkeypatch.setattr(B.smtplib, "SMTP_SSL", FakeSMTP)
    B.notify(_state(), "pool-slot", ["x"])
    body = FakeSMTP.instances[-1].messages[-1].get_content()
    assert "Self-check" in body


def test_every_template_key_exists_in_both_languages():
    en, zh = B.ALERT_TEXT["en"], B.ALERT_TEXT["zh"]
    assert set(en) == set(zh)


def test_chinese_labels_align_with_latin_ones():
    """A CJK header counts 4 characters but occupies 8 columns; padding must use display
    width or every Chinese table comes out ragged."""
    headers = ["池", "名称", "说明"]
    rows = [["a", "common/4C", "最紧"], ["b", "common-16C-long", "正常"]]
    lines = B.fmt_table(headers, rows, [6, 18, 8])

    def boundary(line, cell, after=0):
        idx = line.index(cell, after)
        return B.display_width(line[:idx])

    def starts(line, cells):
        out, cursor = [], 0
        for cell in cells:
            pos = boundary(line, cell, cursor)
            out.append(pos)
            cursor = line.index(cell, cursor) + len(cell)
        return out

    header_starts = starts(lines[0], headers)
    for row, line in zip(rows, lines[2:]):
        assert starts(line, row) == header_starts, line


def test_display_width_counts_wide_characters_as_two():
    assert B.display_width("pool") == 4
    assert B.display_width("池") == 2
    assert B.display_width("已用/上限") == 9
    assert B.display_width("") == 0


def test_numeric_columns_are_right_aligned():
    lines = B.fmt_table(["pool", "free"], [["a", "2"], ["long-name", "10"]], [12, 6])
    assert lines[2].endswith("     2") and lines[3].endswith("    10")


def test_numeric_column_end_positions_match_across_rows():
    """Right-aligned numbers must end on the same display column in the header and in every
    row — that is what makes the digits stack in the mail."""
    headers = ["pool", "used/cap", "slots"]
    rows = [["common/4C", "137/138", "99.3%"], ["common/16C", "1099/1266", "86.8%"]]
    lines = B.fmt_table(headers, rows, [14, 12, 9])

    def cell_end(line, cell):
        idx = line.index(cell)
        return B.display_width(line[:idx]) + B.display_width(cell)

    for col in (1, 2):                      # both numeric -> right aligned
        expected = cell_end(lines[0], headers[col])
        for row, line in zip(rows, lines[2:]):
            assert cell_end(line, row[col]) == expected, (col, line)
