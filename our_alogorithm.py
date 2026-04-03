import math
import psutil
import os
import random as rd

from simulation_core import (
    CAR_METADATA,
    DEFAULT_CONFIG,
    NUM_RUNS,
    POINT_STORE,
    POINT_GAS,
    bfs_path,
    run_many,
    summarize_in_memory,
)

# run on p-core only
p = psutil.Process(os.getpid())
p.cpu_affinity(list(range(12)))

# rd.seed(123456)

def simulate_car_pdf(car_task, points, connections, car_meta, fuel_price):
    points_by_id = {p['id']: p for p in points}
    min_acc, max_acc = car_meta['acceleration_0_100']
    min_eff, max_eff = car_meta['fuel_efficiency']
    fuel_capacity = car_meta['fuel_capacity']
    fuel_efficiency = (min_eff + max_eff) / 2
    acceleration = (min_acc + max_acc) / 2
    max_speed = 100 / acceleration * 3.6

    car_log: list[str] = []
    current_fuel_price = fuel_price

    # PDF-style parameters
    fn = 0.1
    fi = 0.1
    esp_d = 0.3
    eT = 0.1
    qag = 1.0
    qs_reserve = fuel_capacity * 0.10

    def remaining_range_km(qd):
        den = fn + fi * esp_d + eT * qag
        if den <= 0:
            return 0.0
        return max(0.0, (qd - qs_reserve) / den)

    def refuel_at(point, fuel_state):
        nonlocal current_fuel_price
        station_price = point.get('fuel_price', fuel_price)
        current_fuel_price = station_price
        car_log.append(
            f'Refuel at {point["id"]} ({point.get("brand", "n/a")}), price={station_price:.2f} THB/L'
        )
        return fuel_capacity

    def drive_path(path_ids, fuel_state):
        nonlocal total_distance, c2_fuel, c2_extra, cause
        fuel = fuel_state
        for j in range(1, len(path_ids)):
            p1 = points_by_id[path_ids[j - 1]]
            p2 = points_by_id[path_ids[j]]
            dist = math.hypot(p2['x'] - p1['x'], p2['y'] - p1['y'])
            leg_km = dist / 1000.0

            while True:
                if fuel <= qs_reserve:
                    cause = f'Fuel below reserve before leg {p1["id"]}->{p2["id"]}'
                    return fuel
                range_km = remaining_range_km(fuel)
                if leg_km <= range_km or p2['type'] == POINT_GAS:
                    break

                gas_stations = [p for p in points if p['type'] == POINT_GAS]
                if not gas_stations:
                    cause = 'No gas stations available'
                    return fuel
                # Cost = separate distance weight + price weight
                # Weighting price more heavily so our algo actively seeks cheaper fuel
                # K_ab = a×dist_km + b×station_price  (b > a so price-sensitive selection)
                reachable_gas = [g for g in gas_stations
                                 if bfs_path(connections, p1['id'], g['id'])]
                if not reachable_gas:
                    cause = f'No route to any gas station from {p1["id"]}'
                    return fuel
                def gas_pdf_cost(g):
                    d_km = math.hypot(p1['x'] - g['x'], p1['y'] - g['y']) / 1000.0
                    station_price = g.get('fuel_price', fuel_price)
                    # a=0.2 (distance weight in km), b=1.0 (price weight in THB/L)
                    # even more price-sensitive; will detour a bit farther for cheaper fuel
                    return 0.2 * d_km + station_price
                nearest_gas = min(reachable_gas, key=gas_pdf_cost)
                path_to_gas = bfs_path(connections, p1['id'], nearest_gas['id'])
                if not path_to_gas:
                    cause = f'No route to gas station from {p1["id"]}'
                    return fuel
                for k in range(1, len(path_to_gas)):
                    g1 = points_by_id[path_to_gas[k - 1]]
                    g2 = points_by_id[path_to_gas[k]]
                    distg = math.hypot(g2['x'] - g1['x'], g2['y'] - g1['y'])
                    total_distance += distg
                    speedg = min(max_speed, 25)
                    jamg = rd.random() < traffic_jam_prob
                    jamg_mult = traffic_jam_fuel_multiplier if jamg else 1.0
                    fuelg_used = (distg / 1000.0) / fuel_efficiency * jamg_mult
                    fuel -= fuelg_used
                    c2_fuel += fuelg_used * current_fuel_price
                    c2_extra += (distg / 1000.0) * extra_km_rate
                    jamg_note = f' [TRAFFIC JAM x{jamg_mult}]' if jamg else ''
                    car_log.append(
                        f'Detour {g1["id"]}->{g2["id"]}: {distg:.2f}m, speed={speedg:.2f}m/s, '
                        f'fuel_used={fuelg_used:.3f}L, fuel_left={fuel:.2f}L{jamg_note}'
                    )
                    if fuel <= 0:
                        cause = f'Out of fuel at {g2["id"]} during detour'
                        return fuel
                    if g2['type'] == POINT_GAS:
                        fuel = refuel_at(g2, fuel)
                        break
                if cause:
                    return fuel

            speed = min(max_speed, 25)
            jam = rd.random() < traffic_jam_prob
            if jam:
                # Try to find an alternate path that avoids this jammed leg
                alt_path = bfs_path(connections, p1['id'], p2['id'], exclude_edge=(p1['id'], p2['id']))
                if alt_path and len(alt_path) > 2:
                    alt_dist = sum(
                        math.hypot(points_by_id[alt_path[k+1]]['x'] - points_by_id[alt_path[k]]['x'],
                                   points_by_id[alt_path[k+1]]['y'] - points_by_id[alt_path[k]]['y'])
                        for k in range(len(alt_path) - 1)
                    )
                    direct_fuel = (dist / 1000.0) / fuel_efficiency * traffic_jam_fuel_multiplier
                    alt_fuel = (alt_dist / 1000.0) / fuel_efficiency
                    if alt_fuel < direct_fuel:
                        car_log.append(
                            f'JAM REPLAN {p1["id"]}->{p2["id"]}: avoided jam, '
                            f'taking alt route ({len(alt_path)-1} hops, {alt_dist:.2f}m vs {dist:.2f}m direct)'
                        )
                        fuel = drive_path(alt_path, fuel)
                        if cause:
                            return fuel
                        continue
                # No useful alternate — take the jammed leg
                jam_mult = traffic_jam_fuel_multiplier
                jammed_edges.add((p1['id'], p2['id']))
                jammed_edges.add((p2['id'], p1['id']))  # track both directions to avoid reusing the road
                car_log.append(
                    f'JAM (no cheaper alt) {p1["id"]}->{p2["id"]}: {dist:.2f}m [TRAFFIC JAM x{jam_mult}]'
                )
            else:
                jam_mult = 1.0
            total_distance += dist
            fuel_used = (dist / 1000.0) / fuel_efficiency * jam_mult
            fuel -= fuel_used
            c2_fuel += fuel_used * current_fuel_price
            c2_extra += leg_km * extra_km_rate
            jam_note = f' [TRAFFIC JAM x{jam_mult}]' if jam_mult > 1.0 else ''
            car_log.append(
                f'From {p1["id"]} to {p2["id"]}: {dist:.2f}m, speed={speed:.2f}m/s, '
                f'fuel_used={fuel_used:.3f}L, fuel_left={fuel:.2f}L{jam_note}'
            )
            if fuel <= 0:
                cause = f'Out of fuel at {p2["id"]}'
                return fuel
            if p2['type'] == POINT_GAS:
                fuel = refuel_at(p2, fuel)
        return fuel

    def go_to(dest_point, fuel_state, current_id):
        path_ids = bfs_path(connections, current_id, dest_point['id'], exclude_edges=jammed_edges if jammed_edges else None)
        if not path_ids:
            # Fallback: ignore jammed edges if no path found avoiding them
            path_ids = bfs_path(connections, current_id, dest_point['id'])
        if not path_ids:
            return None, f'No route from {current_id} to {dest_point["id"]}'
        new_fuel = drive_path(path_ids, fuel_state)
        if cause:
            return new_fuel, cause
        return new_fuel, ''

    launch = car_task['launch']
    get_point = car_task['get_point']
    stores_route = car_task.get('stores_route', [])
    shopping_list = car_task.get('shopping_list', [])

    # Prefer pre-selected covering stores; only expand to all stores if needed
    preferred_store_ids = {s['id'] for s in stores_route}

    remaining = {item['name']: {'qty': item['qty'], 'buy_price': item['buy_price'], 'category': item['category']} for item in shopping_list}
    fuel = fuel_capacity
    total_distance = 0.0
    total_budget = 0.0
    cause = ''
    partial_reason = ''

    # --- Cost model params ---
    static_cost = car_task.get('static_cost_per_car', 0.0)
    extra_km_rate = car_task.get('extra_km_cost_per_km', 0.0) if car_task.get('extra_km_cost_enabled', False) else 0.0
    traffic_jam_prob = car_task.get('traffic_jam_prob', 0.05)
    traffic_jam_fuel_multiplier = car_task.get('traffic_jam_fuel_multiplier', 1.5)
    c1 = static_cost
    c2_fuel = 0.0
    c2_extra = 0.0
    c3_entrance = 0.0
    c3_items = 0.0

    current_point = launch
    visited_stores = set()
    price_memory: dict[str, float] = {}  # Option 4: item_name -> best price seen so far
    jammed_edges: set[tuple[int, int]] = set()  # Option 3: edges that had jams this run

    while remaining:
        all_candidates = [p for p in points if p['type'] == POINT_STORE and any(
            inv_name in remaining and inv_data.get('qty', 0) > 0 for inv_name, inv_data in p.get('inventory', {}).items())]
        if not all_candidates:
            partial_reason = 'Partial success: no stores with remaining items'
            break
        # Prefer stores in the pre-selected covering route; fall back to all stores if none available
        preferred = [s for s in all_candidates if s['id'] in preferred_store_ids]
        candidates = preferred if preferred else all_candidates

        # Entrance fee filter (Option 2): skip top-third highest-fee stores when cheaper alternatives cover same items
        all_fees = sorted(st.get('entrance_fee', 0.0) for st in candidates)
        fee_cutoff = all_fees[int(len(all_fees) * 0.67)] if len(all_fees) >= 3 else float('inf')
        filtered = [st for st in candidates if st.get('entrance_fee', 0.0) <= fee_cutoff]
        # Only apply filter if it doesn't eliminate all coverage for some item
        remaining_names = set(remaining.keys())
        filtered_coverage = set(n for st in filtered for n in remaining_names if st.get('inventory', {}).get(n, {}).get('qty', 0) > 0)
        if filtered_coverage >= remaining_names:
            candidates = filtered  # safe to drop high-fee stores

        # Option 5+4: composite score — reward covering more items with lower fees + price memory + sell revenue
        max_fee = max((st.get('entrance_fee', 0.0) for st in candidates), default=1.0)
        def score_store(st):
            dist_km = math.hypot(current_point['x'] - st['x'], current_point['y'] - st['y']) / 1000.0
            inv = st.get('inventory', {})
            items_covered = sum(1 for name in remaining if inv.get(name, {}).get('qty', 0) > 0)
            entry_fee = st.get('entrance_fee', 0.0)
            # Normalized consolidation: items covered weighted by fee savings vs max fee
            fee_saving_ratio = (max_fee - entry_fee) / (max_fee + 1.0)
            consolidation_bonus = items_covered * (1.0 + 1.2 * fee_saving_ratio)
            # Option 4: price savings vs known prices
            price_savings = 0.0
            for name in remaining:
                if name in price_memory and inv.get(name, {}).get('qty', 0) > 0:
                    store_price = inv[name].get('price', price_memory[name])
                    price_savings += max(0.0, price_memory[name] - store_price)
            # Option 1: sell revenue bonus — prefer stores where buying fully satisfies an item → earn sell revenue back
            sell_revenue_bonus = 0.0
            for name, need in remaining.items():
                inv_row = inv.get(name, {})
                if inv_row.get('qty', 0) >= need['qty']:
                    sell_revenue_bonus += need['qty'] * inv_row.get('sell_price', 0.0)
            return dist_km - 2.5 * consolidation_bonus - 4.0 * price_savings - 0.5 * sell_revenue_bonus

        candidates = sorted(candidates, key=score_store)
        next_store = None
        for cand in candidates:
            if cand['id'] in visited_stores:
                continue
            if bfs_path(connections, current_point['id'], cand['id']):
                next_store = cand
                break
        if not next_store:
            partial_reason = 'Partial success: remaining items unreachable'
            break

        fuel, err = go_to(next_store, fuel, current_point['id'])
        if err:
            cause = err
            break
        visited_stores.add(next_store['id'])
        c3_entrance += next_store.get('entrance_fee', 0.0)
        current_point = next_store

        inventory = next_store.get('inventory', {})
        purchased_items = []
        for name, need in list(remaining.items()):
            inv_row = inventory.get(name, {})
            available = inv_row.get('qty', 0)
            if available <= 0:
                continue
            buy_qty = min(need['qty'], available)
            inventory[name]['qty'] -= buy_qty
            need['qty'] -= buy_qty
            spent = buy_qty * inv_row.get('price', need['buy_price'])
            revenue = 0.0
            if inventory[name]['qty'] <= 0:
                revenue = buy_qty * inv_row.get('sell_price', 0.0)
            c3_items += spent - revenue
            purchased_items.append((name, buy_qty, spent, revenue))
            if need['qty'] <= 0:
                remaining.pop(name, None)
        if purchased_items:
            details = ', '.join(f'{n}x{q} (cost {cost:.2f} THB' + (f', revenue {rev:.2f} THB' if rev else '') + ')' for n, q, cost, rev in purchased_items)
            car_log.append(f'Buy at store {next_store["id"]}: {details}')
            # Option 4: update price memory with actual prices paid
            for name, buy_qty, spent, revenue in purchased_items:
                inv_row = inventory.get(name, {})
                actual_price = inv_row.get('price', 0.0)
                if actual_price > 0 and (name not in price_memory or actual_price < price_memory[name]):
                    price_memory[name] = actual_price

    if not cause:
        fuel, err = go_to(get_point, fuel, current_point['id'])
        if err:
            cause = err

    if not cause and remaining:
        partial_reason = 'Partial success: missing items ' + ', '.join(f'{n}:{v["qty"]}' for n, v in remaining.items())

    if cause:
        final_cause = cause
    elif partial_reason:
        final_cause = partial_reason
    else:
        final_cause = 'Completed quest'

    total_budget = c1 + c2_fuel + c2_extra + c3_entrance + c3_items
    return {
        'cause': final_cause,
        'distance': total_distance,
        'budget': total_budget,
        'log': car_log,
        'c1': c1,
        'c2_fuel': c2_fuel,
        'c2_extra': c2_extra,
        'c3_entrance': c3_entrance,
        'c3_items': c3_items,
    }


def main():
    # rd.seed(200)
    config = DEFAULT_CONFIG.copy()
    config['car_metadata'] = CAR_METADATA
    config['log_dir'] = 'logs_our'
    config['delete_failed_logs'] = True
    config['algo_name'] = 'NewAlgo'
    summary = run_many(
        num_runs=NUM_RUNS,
        strategy=simulate_car_pdf,
        summarizer=summarize_in_memory,
        config=config,
    )
    return summary

if __name__ == '__main__':
    main()

if __name__ == 'ouralgo':
    main()