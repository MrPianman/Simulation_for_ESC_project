import math
import psutil
import os
import random as rd

from simulation_core import (
    CAR_METADATA,
    DEFAULT_CONFIG,
    NUM_RUNS,
    POINT_GAS,
    POINT_STORE,
    bfs_path,
    run_many,
    summarize_in_memory,
)

# rd.seed(6767)

# run on p-core only
p = psutil.Process(os.getpid())
p.cpu_affinity(list(range(12)))

def simulate_car_threshold(car_task, points, connections, car_meta, threshold=0.2, fuel_price=35):
    launch = car_task['launch']
    get_point = car_task['get_point']
    stores_route = list(car_task.get('stores_original') or car_task.get('stores_route', []))
    shopping_list = car_task.get('shopping_list', [])

    # Static but reordered once by nearest-neighbor to shorten the tour.
    ordered: list = []
    remaining_stores = list(stores_route)
    current_pt = launch
    while remaining_stores:
        remaining_stores.sort(key=lambda st: math.hypot(current_pt['x'] - st['x'], current_pt['y'] - st['y']))
        next_store = remaining_stores.pop(0)
        ordered.append(next_store)
        current_pt = next_store

    route = [launch] + ordered + [get_point]

    car_log: list[str] = []
    current_fuel_price = fuel_price

    points_by_id = {p['id']: p for p in points}
    min_acc, max_acc = car_meta['acceleration_0_100']
    min_eff, max_eff = car_meta['fuel_efficiency']
    fuel_capacity = car_meta['fuel_capacity']
    fuel_efficiency = (min_eff + max_eff) / 2
    acceleration = (min_acc + max_acc) / 2
    max_speed = 100 / acceleration * 3.6

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

    def refuel_at(point, fuel_state):
        nonlocal current_fuel_price
        station_price = point.get('fuel_price', fuel_price)
        current_fuel_price = station_price
        car_log.append(
            f'Refuel at {point["id"]} ({point.get("brand", "n/a")}), price={station_price:.2f} THB/L'
        )
        return fuel_capacity

    idx = 1
    while idx < len(route):
        src = route[idx - 1]
        dst = route[idx]
        path_ids = bfs_path(connections, src['id'], dst['id'])
        if not path_ids:
            cause = f'No route from {src["id"]} to {dst["id"]}'
            break
        for j in range(1, len(path_ids)):
            p1 = points_by_id[path_ids[j - 1]]
            p2 = points_by_id[path_ids[j]]
            dist = math.hypot(p2['x'] - p1['x'], p2['y'] - p1['y'])
            total_distance += dist
            speed = min(max_speed, 25)
            jam = rd.random() < traffic_jam_prob
            jam_mult = traffic_jam_fuel_multiplier if jam else 1.0
            fuel_used = (dist / 1000.0) / fuel_efficiency * jam_mult
            fuel -= fuel_used
            c2_fuel += fuel_used * current_fuel_price
            c2_extra += (dist / 1000.0) * extra_km_rate
            jam_note = f' [TRAFFIC JAM x{jam_mult}]' if jam else ''
            car_log.append(
                f'From {p1["id"]} to {p2["id"]}: {dist:.2f}m, speed={speed:.2f}m/s, '
                f'fuel_used={fuel_used:.3f}L, fuel_left={fuel:.2f}L{jam_note}'
            )

            if fuel <= fuel_capacity * threshold and p2['type'] != POINT_GAS:
                gas_stations = [p for p in points if p['type'] == POINT_GAS]
                if not gas_stations:
                    cause = 'No gas stations available'
                    break
                nearest_gas = min(gas_stations, key=lambda g: math.hypot(p2['x'] - g['x'], p2['y'] - g['y']))
                path_to_gas = bfs_path(connections, p2['id'], nearest_gas['id'])
                if not path_to_gas:
                    cause = f'No route to gas station from {p2["id"]}'
                    break
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
                    if g2['type'] == POINT_GAS:
                        fuel = refuel_at(g2, fuel)
                        break
                break

            if fuel <= 0:
                cause = f'Out of fuel at {p2["id"]}'
                break
            if p2['type'] == POINT_GAS:
                fuel = refuel_at(p2, fuel)
        if cause:
            break

        if dst['type'] == POINT_STORE:
            inventory = dst.get('inventory', {})
            c3_entrance += dst.get('entrance_fee', 0.0)
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
                car_log.append(f'Buy at store {dst["id"]}: {details}')

            # If all items are fulfilled, head directly to get_point and stop visiting remaining stores.
            if not remaining and dst['id'] != get_point['id']:
                path_ids = bfs_path(connections, dst['id'], get_point['id'])
                if not path_ids:
                    cause = f'No route from {dst["id"]} to get_point {get_point["id"]}'
                    break
                for j2 in range(1, len(path_ids)):
                    p1 = points_by_id[path_ids[j2 - 1]]
                    p2 = points_by_id[path_ids[j2]]
                    dist2 = math.hypot(p2['x'] - p1['x'], p2['y'] - p1['y'])
                    total_distance += dist2
                    speed2 = min(max_speed, 25)
                    fuel_used2 = (dist2 / 1000.0) / fuel_efficiency
                    fuel -= fuel_used2
                    c2_fuel += fuel_used2 * current_fuel_price
                    c2_extra += (dist2 / 1000.0) * extra_km_rate
                    car_log.append(
                        f'From {p1["id"]} to {p2["id"]}: {dist2:.2f}m, speed={speed2:.2f}m/s, '
                        f'fuel_used={fuel_used2:.3f}L, fuel_left={fuel:.2f}L'
                    )
                    if fuel <= fuel_capacity * threshold and p2['type'] != POINT_GAS:
                        gas_stations = [p for p in points if p['type'] == POINT_GAS]
                        if not gas_stations:
                            cause = 'No gas stations available'
                            break
                        nearest_gas = min(gas_stations, key=lambda g: math.hypot(p2['x'] - g['x'], p2['y'] - g['y']))
                        path_to_gas = bfs_path(connections, p2['id'], nearest_gas['id'])
                        if not path_to_gas:
                            cause = f'No route to gas station from {p2["id"]}'
                            break
                        for k in range(1, len(path_to_gas)):
                            g1 = points_by_id[path_to_gas[k - 1]]
                            g2 = points_by_id[path_to_gas[k]]
                            distg = math.hypot(g2['x'] - g1['x'], g2['y'] - g1['y'])
                            total_distance += distg
                            speedg = min(max_speed, 25)
                            fuelg_used = (distg / 1000.0) / fuel_efficiency
                            fuel -= fuelg_used
                            c2_fuel += fuelg_used * current_fuel_price
                            c2_extra += (distg / 1000.0) * extra_km_rate
                            car_log.append(
                                f'Detour {g1["id"]}->{g2["id"]}: {distg:.2f}m, speed={speedg:.2f}m/s, '
                                f'fuel_used={fuelg_used:.3f}L, fuel_left={fuel:.2f}L'
                            )
                            if g2['type'] == POINT_GAS:
                                fuel = refuel_at(g2, fuel)
                                break
                        break

                    if fuel <= 0:
                        cause = f'Out of fuel at {p2["id"]}'
                        break
                    if p2['type'] == POINT_GAS:
                        fuel = refuel_at(p2, fuel)
                break  # exit main loop after heading home

        idx += 1

    if not cause and remaining:
        partial_reason = 'Partial success: missing items ' + ', '.join(f'{n}:{v["qty"]}' for n, v in remaining.items())

    if cause:
        final_cause = cause
    elif remaining:
        # No fallback to other stores — partial coverage is a failure
        final_cause = 'Partial success: missing items ' + ', '.join(f'{n}:{v["qty"]}' for n, v in remaining.items())
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
    # rd.seed(100)
    config = DEFAULT_CONFIG.copy()
    config['car_metadata'] = CAR_METADATA
    config['log_dir'] = 'logs_old'
    config['delete_fuel_fail_logs'] = True
    config['delete_failed_logs'] = True
    config['algo_name'] = 'OldAlgo'
    summary = run_many(
        num_runs=NUM_RUNS,
        strategy=simulate_car_threshold,
        summarizer=summarize_in_memory,
        config=config,
    )
    return summary


if __name__ == '__main__':
	main()
    
if __name__ == 'oldalgo':
    main()