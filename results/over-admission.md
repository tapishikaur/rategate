# Over-admission vs sync interval, 2 gateways

Measured 2026-10-28 on 2 CPUs (Linux 6.8.0-1064-azure x86_64), the whole Docker Compose stack and the load tool on the same machine.
4 API keys, each limited to 100 requests/s with a burst of 100. Steady scenarios offer 300/s per key (3x the limit)
for 60s; the burst scenario sends 500 requests per key at once after a quiet spell.

- **Allowed** = burst + rate x T: what one perfect token bucket, starting full, would admit (summed over the keys).
- **Admitted** = what reached the backend (its own count).
- **Over-admission** = (admitted - allowed) / allowed.
- **Worst 1 s** = most requests admitted for one key in any 1-second window; one exact bucket admits at most rate + burst = 200.
- **Burst overshoot** = requests admitted per key in the burst beyond the burst size.

| Setting | Scenario | Allowed | Admitted | Over-admission | Worst 1 s (exact: 200) | Burst overshoot per key |
|---|---|---|---|---|---|---|
| exact | steady-50/50 | 24399 | 24364 | -0.1% | 194 |  |
| exact | steady-90/10 | 24399 | 24393 | -0.0% | 199 |  |
| exact | burst | 788 | 803 | +1.9% |  | 101 |
| sync 10ms | steady-50/50 | 24399 | 24394 | -0.0% | 200 |  |
| sync 10ms | steady-90/10 | 24399 | 24394 | -0.0% | 199 |  |
| sync 10ms | burst | 569 | 592 | +4.0% |  | 48 |
| sync 50ms | steady-50/50 | 24399 | 24390 | -0.0% | 200 |  |
| sync 50ms | steady-90/10 | 24399 | 24393 | -0.0% | 201 |  |
| sync 50ms | burst | 597 | 650 | +8.9% |  | 62 |
| sync 100ms | steady-50/50 | 24399 | 24403 | +0.0% | 205 |  |
| sync 100ms | steady-90/10 | 24399 | 24394 | -0.0% | 201 |  |
| sync 100ms | burst | 571 | 713 | +24.8% |  | 78 |
| sync 250ms | steady-50/50 | 24399 | 24358 | -0.2% | 192 |  |
| sync 250ms | steady-90/10 | 24399 | 24389 | -0.0% | 201 |  |
| sync 250ms | burst | 655 | 849 | +29.5% |  | 112 |
| sync 500ms | steady-50/50 | 24399 | 24335 | -0.3% | 234 |  |
| sync 500ms | steady-90/10 | 24399 | 24310 | -0.4% | 189 |  |
| sync 500ms | burst | 536 | 755 | +40.7% |  | 89 |
| sync 1s | steady-50/50 | 24399 | 24260 | -0.6% | 325 |  |
| sync 1s | steady-90/10 | 24399 | 24159 | -1.0% | 228 |  |
| sync 1s | burst | 589 | 947 | +60.8% |  | 137 |
| local | steady-50/50 | 24399 | 48779 | +99.9% | 329 |  |
| local | steady-90/10 | 24399 | 31602 | +29.5% | 231 |  |
| local | burst | 707 | 1380 | +95.1% |  | 245 |

## Reading the table

- **Steady overload:** every sync interval from 10 ms to 1 s stays within about ±1% of one exact bucket over the
  minute. The debt mechanism pays back any overspend, so the error doesn't accumulate. Local mode, with no sharing,
  lets through about 2× (+99.9%) with an even split.
- **The interval shows up in short windows.** The worst 1-second window grows with the interval, from about 200 (what
  one exact bucket allows) at 10–100 ms to 325 at 1 s. That's the burst the gateways can spend between two syncs.
- **Bursts after a quiet spell** are the largest error, and they grow with the interval: +4% at 10 ms, +25% at 100 ms,
  +61% at 1 s, +95% with no sharing. After a quiet spell every gateway's view is a full bucket, and both spend from it
  until the next sync tells them about the other. The debt this leaves is then paid back.

**Caveat: the burst rows depend on this machine.** With the whole stack and the load tool on 2 CPUs, the tool couldn't
send the 2,000-request bursts all at once. It reported requests going out up to about 1 s late in every burst run, and
up to 178 ms late in 3 of the 16 steady runs. A burst spread over that much time gives the gateways time to refill
and sync mid-burst. That is why "allowed" (burst + rate × the send span) is above the burst alone in those rows. It's
also why exact mode shows +1.9%: requests reached the gateways after their recorded send times, so the send span
slightly understates the time the gateways saw. The steady rows (60 s each) are barely affected.
