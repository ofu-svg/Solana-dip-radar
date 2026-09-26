from solana_dip_radar import drawdown_percent, threshold_for

tests = [
    (0.002, 0.010),   # 80%
    (0.0005, 0.010),  # 95%
    (0.0001, 0.010),  # 99%
]
for price, ath in tests:
    dd = drawdown_percent(price, ath)
    print(price, ath, dd, threshold_for(dd))
