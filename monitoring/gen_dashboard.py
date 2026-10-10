# Generates grafana/dashboards/brokers.json: python3 monitoring/gen_dashboard.py monitoring/grafana/dashboards/brokers.json
import json, sys

DS = {"type": "prometheus", "uid": "prometheus"}
panels = []
next_id = [1]


def target(expr, legend, ref):
    return {"datasource": DS, "expr": expr, "legendFormat": legend, "refId": ref, "range": True}


def panel(kind, title, x, y, w, h, targets, unit=None, description=None, log=False, overrides=None, options=None, extra=None):
    p = {
        "id": next_id[0], "type": kind, "title": title, "datasource": DS,
        "gridPos": {"x": x, "y": y, "w": w, "h": h},
        "targets": [target(e, l, chr(ord("A") + i)) | ({"instant": True, "range": False} if kind == "stat" else {})
                    for i, (e, l) in enumerate(targets)],
        "fieldConfig": {"defaults": {}, "overrides": overrides or []},
        "options": options or {},
    }
    next_id[0] += 1
    if description:
        p["description"] = description
    d = p["fieldConfig"]["defaults"]
    if unit:
        d["unit"] = unit
    if kind == "timeseries":
        d["custom"] = {"lineWidth": 2, "fillOpacity": 8, "showPoints": "never", "spanNulls": False}
        if log:
            d["custom"]["scaleDistribution"] = {"type": "log", "log": 10}
        p["options"] = {"legend": {"displayMode": "list", "placement": "bottom"}, "tooltip": {"mode": "multi", "sort": "desc"}} | p["options"]
    if extra:
        d.update(extra)
    panels.append(p)


def row(title, y):
    panels.append({"id": next_id[0], "type": "row", "title": title, "collapsed": False, "gridPos": {"x": 0, "y": y, "w": 24, "h": 1}, "panels": []})
    next_id[0] += 1


dashed = lambda regex: {"matcher": {"id": "byRegexp", "options": regex}, "properties": [
    {"id": "custom.lineStyle", "value": {"fill": "dash", "dash": [6, 6]}}, {"id": "custom.fillOpacity", "value": 0}]}
red = lambda regex: {"matcher": {"id": "byRegexp", "options": regex}, "properties": [
    {"id": "color", "value": {"mode": "fixed", "fixedColor": "red"}}]}
stat_opts = {"reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": False}, "colorMode": "none", "graphMode": "none", "justifyMode": "center"}
running = 'and on() (max(queuebench_phase{phase!="done"}) == 1)'

# Row 0: what runs right now.
panel("stat", "Профиль", 0, 0, 6, 4, [(f"max by (profile) (queuebench_run_info) {running}", "{{profile}}")],
      options=stat_opts | {"textMode": "name"}, description="Пусто между профилями и после прогона")
panel("stat", "Фаза", 6, 0, 5, 4, [("max by (phase) (queuebench_phase == 1)", "{{phase}}")],
      options=stat_opts | {"textMode": "name"}, description="throughput: prime → fill → drain → steady/catch_up; chaos: prime → steady (сбой внутри) → catch_up")
panel("stat", "Целевая скорость", 11, 0, 5, 4, [(f"max(queuebench_target_rate) {running}", "цель")],
      unit="short", options=stat_opts | {"textMode": "value"})
panel("stat", "Убит узел", 16, 0, 8, 4, [("max by (target) (queuebench_fault == 1)", "{{target}}")],
      options=stat_opts | {"textMode": "name", "colorMode": "background"},
      extra={"color": {"mode": "fixed", "fixedColor": "red"}, "noValue": "все узлы живы"},
      description="Chaos: узел, который сейчас лежит после kill -9")

# Row 1: flow.
row("Поток сообщений", 4)
panel("timeseries", "Сообщений в секунду: подтверждено, прочитано, цель", 0, 5, 16, 9, [
    ("sum by (profile) (rate(queuebench_produced_total[5s]))", "{{profile}} подтверждено"),
    ("sum by (profile) (rate(queuebench_consumed_total[5s]))", "{{profile}} прочитано"),
    ("max by (profile) (queuebench_target_rate) and on(profile) (max by (profile) (queuebench_phase{phase=\"steady\"}) == 1)", "{{profile}} цель"),
], unit="short", overrides=[dashed(".* цель")],
    description="Насыщение: «прочитано» ниже цели, backlog растёт. Сбой: «подтверждено» проваливается в ноль.")
panel("timeseries", "Backlog (подтверждено − прочитано)", 16, 5, 8, 9, [
    ("sum by (profile) (queuebench_produced_total) - sum by (profile) (queuebench_consumed_total)", "{{profile}}"),
], unit="short")

# Row 2: faults.
row("Сбой и его последствия", 14)
panel("timeseries", "Ошибки отправки в секунду и узел лежит", 0, 15, 12, 8, [
    ("sum by (profile) (rate(queuebench_send_failed_total[5s]))", "{{profile}} не подтверждено/с"),
    ("max by (target) (queuebench_fault)", "{{target}} лежит"),
], unit="short", overrides=[{"matcher": {"id": "byRegexp", "options": ".* лежит"}, "properties": [
    {"id": "color", "value": {"mode": "fixed", "fixedColor": "red"}},
    {"id": "custom.axisPlacement", "value": "right"}, {"id": "max", "value": 1}, {"id": "min", "value": 0},
    {"id": "custom.fillOpacity", "value": 25}, {"id": "custom.lineInterpolation", "value": "stepAfter"},
    {"id": "unit", "value": "none"}]}],
    description="Send вернул ошибку или не дождался ack за -send-timeout. Такие сообщения могут всё равно дойти: это «доставлено без подтверждения» в отчёте.")
panel("timeseries", "Дубли в секунду (повторные доставки)", 12, 15, 12, 8, [
    ("sum by (profile) (rate(queuebench_duplicates_total[5s]))", "{{profile}}"),
], unit="short", description="После сбоя неподтверждённые воркерами сообщения приходят снова: at-least-once.")

# Row 3: latency.
row("Задержка", 23)
panel("timeseries", "e2e: от плановой отправки до обработки (p50, p99)", 0, 24, 12, 9, [
    ("histogram_quantile(0.5, sum by (le, profile) (rate(queuebench_e2e_seconds_bucket[5s])))", "{{profile}} p50"),
    ("histogram_quantile(0.99, sum by (le, profile) (rate(queuebench_e2e_seconds_bucket[5s])))", "{{profile}} p99"),
], unit="s", log=True, description="Округлено до бакетов гистограммы (×1.5). Точные значения — в отчёте.")
panel("timeseries", "Одна пачка: send и receive p99", 12, 24, 12, 9, [
    ("histogram_quantile(0.99, sum by (le, profile) (rate(queuebench_send_seconds_bucket[5s])))", "{{profile}} send"),
    ("histogram_quantile(0.99, sum by (le, profile) (rate(queuebench_receive_seconds_bucket[5s])))", "{{profile}} receive"),
], unit="s", log=True)

# Row 4: resources.
row("Ресурсы контейнеров", 33)
panel("timeseries", "CPU, % (100 = одно ядро)", 0, 34, 12, 8, [("queuebench_container_cpu_percent", "{{container}}")],
      unit="percent", description="Одиночный брокер — 2 ядра, узел кластера — 1.5, loadgen — 3. loadgen около 300%: упёрлись в генератор.")
panel("timeseries", "Память", 12, 34, 12, 8, [("queuebench_container_memory_bytes", "{{container}}")], unit="bytes")

# Row 5: inside the broker.
row("Внутри брокера", 42)
panel("timeseries", "Очередь в брокере", 0, 43, 12, 8, [
    ("queuebench_broker_stat{stat=~\"messages|messages_ready|messages_unacked|lag|ack_pending\"}", "{{profile}} {{stat}}"),
], unit="short", description="Kafka: lag группы. RabbitMQ: готовые и выданные без ack. NATS: сообщений в стриме и ожидающих ack.")
panel("timeseries", "Живые реплики", 12, 43, 12, 8, [
    ("queuebench_broker_stat{stat=~\"online_members|current_replicas|min_isr_size\"}", "{{profile}} {{stat}}"),
], unit="short", description="Kafka: размер ISR (минимум по партициям). RabbitMQ: живые члены quorum-очереди. NATS: реплики стрима, которые догнали лидера.")

dashboard = {
    "uid": "brokerbench", "title": "Broker benchmark: Kafka, RabbitMQ, NATS JetStream",
    "tags": ["brokerbench"], "timezone": "browser", "schemaVersion": 39, "version": 1,
    "refresh": "2s", "time": {"from": "now-30m", "to": "now"}, "editable": True,
    "fiscalYearStartMonth": 0, "graphTooltip": 1, "liveNow": False,
    "timepicker": {"refresh_intervals": ["1s", "2s", "5s", "10s", "30s"]},
    "annotations": {"list": []}, "templating": {"list": []},
    "panels": panels,
}
json.dump(dashboard, open(sys.argv[1], "w", encoding="utf-8", newline="\n"), ensure_ascii=False, indent=2)
print(len(panels), "panels")
