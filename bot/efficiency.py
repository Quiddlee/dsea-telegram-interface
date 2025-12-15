ds = 5
ts = 85

db = 3
tb = 15

ps = 1 / 7

delta_d = ds - db
percent_d = (delta_d / ds) * 100

delta_t = ts - tb
percent_t = (delta_t / ts) * 100

ts_avg = (1 - ps) * ts + ps * 2 * ts
delta_t_avg = ts_avg - tb
percent_t_avg = (delta_t_avg / ts_avg) * 100

print("Ефективність Telegram-бота:")
print(f"1. Дій через сайт: {ds}")
print(f"2. Дій через бота: {db}")
print(f"   → Економія дій: {delta_d} ({percent_d:.1f}%)")

print(f"\n3. Час через сайт: {ts} с")
print(f"4. Час через бота: {tb} с")
print(f"   → Економія часу: {delta_t} с ({percent_t:.2f}%)")

print(f"\n5. Середній час через сайт з урахуванням збоїв: {ts_avg:.1f} с")
print(f"   → Економія часу з урахуванням збоїв: {delta_t_avg:.1f} с ({percent_t_avg:.2f}%)")
