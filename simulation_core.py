import copy
import json
import math
import multiprocessing
import os
import random
from collections import deque
from datetime import datetime
from functools import partial
from typing import Callable, Dict, List, Set

Point = Dict[str, float | int | str]
Connections = Dict[int, Set[int]]

# --- CONFIG PARAMETERS ---
NUM_RUNS = 10000  # default number of simulation rounds
MAP_SIZE = 1000
STORE_COUNT = 80
GAS_COUNT = 70
LAUNCH_COUNT = 10
GET_POINT_COUNT = LAUNCH_COUNT
LOG_DIR = 'logs'
MAX_NEIGHBORS = 1
FUEL_PRICE = 29.94  # THB per liter (Diesel B7)
STATISTIC_COST = 100

# --- COST MODEL (C1 / C2 / C3) ---
STATIC_COST_PER_CAR = STATISTIC_COST    # C1: one-time vehicle use cost per car (THB)
EXTRA_KM_COST_ENABLED = False           # C2: toggle per-km overhead on top of fuel cost
EXTRA_KM_COST_PER_KM = 0              # C2: overhead rate (THB per km)
STORE_ENTRANCE_FEE_MIN = 120             # C3: min entrance fee per store visit (THB)
STORE_ENTRANCE_FEE_MAX = 400             # C3: max entrance fee per store visit (THB)


# --- CAR DATA ---
CAR_METADATA = {
    'weight': 1610,  # kg
    'max_power': 170,  # Ps
    'torque': 405,  # Nm
    'acceleration_0_100': (10.5, 12),  # s
    'fuel_efficiency': (11.7, 13.9),  # km/L
    'fuel_capacity': 80,  # L
    'turn_angle': (30, 40),  # degrees
    'wheel_friction': (0.8, 0.9),
    'length': 5.35,  # m (converted from mm)
    'width': 1.95,  # m (converted from mm)
    'engine': 'RWD 4WD',
}

# --- POINT TYPES ---
POINT_STORE = 'store'
POINT_GAS = 'gas_station'
POINT_LAUNCH = 'launch_point'
POINT_GET = 'get_point'

DEFAULT_CONFIG = {
    'map_size': MAP_SIZE,
    'store_count': STORE_COUNT,
    'gas_count': GAS_COUNT,
    'launch_count': LAUNCH_COUNT,
    'get_point_count': GET_POINT_COUNT,
    'log_dir': LOG_DIR,
    'big_log_subdir': 'big_logs',
    'big_log_every': 1,
    'max_neighbors': MAX_NEIGHBORS,
    'fuel_price_base': FUEL_PRICE,
    'fuel_price_noise_range': 12,  # wider price spread makes smart selection matter more
    'fuel_brand_count': 12,
    'fuel_brand_prices': None,  # optional explicit list overrides base/noise
    'static_cost_per_car': STATIC_COST_PER_CAR,
    'extra_km_cost_enabled': EXTRA_KM_COST_ENABLED,
    'extra_km_cost_per_km': EXTRA_KM_COST_PER_KM,
    'store_entrance_fee_min': STORE_ENTRANCE_FEE_MIN,
    'store_entrance_fee_max': STORE_ENTRANCE_FEE_MAX,
    'car_metadata': CAR_METADATA,
    'market_file': 'marketlist.json',
    'traffic_jam_prob': 0.12,           # probability of traffic jam on any road leg
    'traffic_jam_fuel_multiplier': 2,   # fuel consumption multiplier when in traffic jam
    'delete_failed_logs': False,        # delete log file for any run that did not fully complete
}

# --- Set seed ---

# random.seed(6767)

# --- UTIL FUNCTIONS ---

def ensure_log_dir(log_dir: str) -> None:
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)


def clear_old_logs(log_dir: str) -> None:
    if not os.path.exists(log_dir):
        return
    for fname in os.listdir(log_dir):
        full = os.path.join(log_dir, fname)
        if fname.endswith('.txt'):
            try:
                os.remove(full)
            except OSError:
                pass
        elif os.path.isdir(full) and fname == 'big_logs':
            for bname in os.listdir(full):
                if bname.endswith('.txt'):
                    try:
                        os.remove(os.path.join(full, bname))
                    except OSError:
                        pass

def random_points(config: Dict) -> List[Point]:
    map_size = config['map_size']
    store_count = config['store_count']
    gas_count = config['gas_count']
    launch_count = config['launch_count']
    get_point_count = config.get('get_point_count', launch_count)
    num_points = store_count + gas_count + launch_count + get_point_count

    points = []
    for i in range(num_points):
        x = random.randint(0, int(map_size))
        y = random.randint(0, int(map_size))
        points.append({'id': i, 'x': x, 'y': y})

    types = ([POINT_STORE] * store_count +
             [POINT_GAS] * gas_count +
             [POINT_LAUNCH] * launch_count +
             [POINT_GET] * get_point_count)
    random.shuffle(types)

    brand_prices_cfg = config.get('fuel_brand_prices')
    if brand_prices_cfg:
        brand_prices = list(brand_prices_cfg)
    else:
        base_price = config.get('fuel_price_base', config.get('fuel_price', FUEL_PRICE))
        noise = config.get('fuel_price_noise_range', 0.0)
        brand_count = max(1, int(config.get('fuel_brand_count', 1)))
        brand_prices = [base_price + random.uniform(-noise, noise) for _ in range(brand_count)]

    gas_seen = 0
    for i, t in enumerate(types):
        points[i]['type'] = t
        if t == POINT_GAS:
            brand_idx = gas_seen % len(brand_prices)
            points[i]['brand'] = f'Brand {brand_idx + 1}'
            points[i]['fuel_price'] = brand_prices[brand_idx]
            gas_seen += 1
    return points


def connect_points(points: List[Point], max_neighbors: int) -> Connections:
    connections: Connections = {p['id']: set() for p in points}
    for p in points:
        others = [o for o in points if o['id'] != p['id']]
        others_sorted = sorted(others, key=lambda o: math.hypot(p['x'] - o['x'], p['y'] - o['y']))
        nearest = others_sorted[:max_neighbors]
        for o in nearest:
            connections[p['id']].add(o['id'])
            connections[o['id']].add(p['id'])

    # Guarantee full connectivity: walk from node 0 and bridge any isolated components
    all_ids = set(connections.keys())
    visited: Set[int] = set()
    stack = [next(iter(all_ids))]
    while stack:
        node = stack.pop()
        if node in visited:
            continue
        visited.add(node)
        stack.extend(connections[node] - visited)

    unvisited = all_ids - visited
    while unvisited:
        # Pick closest reachable node to bridge the gap
        pts_by_id = {p['id']: p for p in points}
        u = next(iter(unvisited))
        nearest_reached = min(
            visited,
            key=lambda vid: math.hypot(pts_by_id[u]['x'] - pts_by_id[vid]['x'],
                                       pts_by_id[u]['y'] - pts_by_id[vid]['y'])
        )
        connections[u].add(nearest_reached)
        connections[nearest_reached].add(u)
        # Re-walk from u
        stack = [u]
        while stack:
            node = stack.pop()
            if node in visited:
                continue
            visited.add(node)
            stack.extend(connections[node] - visited)
        unvisited = all_ids - visited

    return connections


def bfs_path(connections: Connections, start_id: int, end_id: int, exclude_edge: tuple | None = None, exclude_edges: set | None = None) -> List[int] | None:
    queue = deque([[start_id]])
    visited = set()
    while queue:
        path = queue.popleft()
        node = path[-1]
        if node == end_id:
            return path
        if node not in visited:
            visited.add(node)
            for neighbor in connections[node]:
                if exclude_edge and (node, neighbor) == exclude_edge:
                    continue
                if exclude_edges and (node, neighbor) in exclude_edges:
                    continue
                new_path = list(path)
                new_path.append(neighbor)
                queue.append(new_path)
    return None


def assign_stores_to_launches(launch_points: List[Point], stores: List[Point]) -> Dict[int, List[Point]]:
    random.shuffle(stores)
    buckets = {lp['id']: [] for lp in launch_points}
    for idx, store in enumerate(stores):
        lp = launch_points[idx % len(launch_points)]
        buckets[lp['id']].append(store)
    return buckets


def load_market_items(market_file: str) -> List[Dict]:
    with open(market_file, 'r', encoding='utf-8') as fh:
        data = json.load(fh)
    items: List[Dict] = []
    for category, rows in data.items():
        for row in rows:
            item = row.copy()
            item['category'] = category
            items.append(item)
    return items


def assign_store_inventories(stores: List[Point], market_items: List[Dict],
                             min_item_types: int = 2, stock_range: tuple[int, int] = (4, 12),
                             price_range: tuple[float, float] = (0.9, 1.2),
                             entrance_fee_range: tuple[float, float] = (0.0, 0.0)) -> None:
    for store in stores:
        sample_size = min(len(market_items), max(min_item_types, random.randint(min_item_types, len(market_items))))
        chosen = random.sample(market_items, sample_size)
        inventory: Dict[str, Dict] = {}
        for item in chosen:
            qty = random.randint(stock_range[0], stock_range[1])
            multiplier = random.uniform(price_range[0], price_range[1])
            inventory[item['name']] = {
                'qty': qty,
                'price': item['buy_price'] * multiplier,
                'sell_price': item['sell_price'],
                'category': item['category'],
            }
        store['inventory'] = inventory
        store['entrance_fee'] = random.uniform(entrance_fee_range[0], entrance_fee_range[1])


def generate_shopping_list(market_items: List[Dict], min_items: int = 3, max_items: int = 6,
                           min_qty: int = 1, max_qty: int = 4) -> List[Dict]:
    count = random.randint(min_items, max_items)
    picks = [random.choice(market_items) for _ in range(count)]
    consolidated: Dict[str, Dict] = {}
    for pick in picks:
        name = pick['name']
        qty = random.randint(min_qty, max_qty)
        if name not in consolidated:
            consolidated[name] = {
                'name': name,
                'category': pick['category'],
                'buy_price': pick['buy_price'],
                'qty': 0,
            }
        consolidated[name]['qty'] += qty
    return list(consolidated.values())


def shopping_list_from_inventory_pool(stores: List[Point], fallback_items: List[Dict],
                                      min_items: int = 3, max_items: int = 6,
                                      min_qty: int = 1, max_qty: int = 4) -> List[Dict]:
    pool: List[Dict] = []
    stock: Dict[str, int] = {}
    seen = set()
    for st in stores:
        inv = st.get('inventory', {})
        for name, row in inv.items():
            stock[name] = stock.get(name, 0) + row.get('qty', 0)
            if name in seen:
                continue
            seen.add(name)
            pool.append({'name': name, 'category': row.get('category', ''), 'buy_price': row.get('price', 0)})
    source = pool if pool else fallback_items
    if not source:
        return []

    count = random.randint(min_items, max_items)
    consolidated: Dict[str, Dict] = {}
    # Only select items that still have stock
    available_names = [item['name'] for item in source if stock.get(item['name'], 0) > 0]
    if not available_names:
        available_names = [item['name'] for item in source]
    for _ in range(count):
        if not available_names:
            break
        name = random.choice(available_names)
        base = next(item for item in source if item['name'] == name)
        max_cap = stock.get(name, max_qty)
        qty = random.randint(min_qty, max_qty)
        qty = min(qty, max_cap)
        if qty <= 0:
            available_names = [n for n in available_names if n != name]
            continue
        stock[name] = max_cap - qty
        if stock[name] <= 0:
            available_names = [n for n in available_names if n != name]
        if name not in consolidated:
            consolidated[name] = {
                'name': name,
                'category': base.get('category', ''),
                'buy_price': base.get('buy_price', 0),
                'qty': 0,
            }
        consolidated[name]['qty'] += qty
    return list(consolidated.values())


def select_covering_stores(all_stores: List[Point], shopping_list: List[Dict], launch: Point,
                           max_stores: int = 6) -> List[Point]:
    if not shopping_list:
        return []
    remaining = {item['name'] for item in shopping_list}
    chosen: List[Point] = []
    candidates = list(all_stores)
    current = launch
    while remaining and candidates and len(chosen) < max_stores:
        candidates.sort(key=lambda st: math.hypot(current['x'] - st['x'], current['y'] - st['y']))
        picked = None
        for st in candidates:
            inv = st.get('inventory', {})
            if any(name in remaining and inv.get(name, {}).get('qty', 0) > 0 for name in remaining):
                picked = st
                break
        if not picked:
            break
        chosen.append(picked)
        current = picked
        candidates = [c for c in candidates if c['id'] != picked['id']]
        inv = picked.get('inventory', {})
        remaining = {name for name in remaining if inv.get(name, {}).get('qty', 0) <= 0}
    return chosen if chosen else []


def pair_launch_to_getpoints(launch_points: List[Point], get_points: List[Point]) -> Dict[int, Point]:
    pairs: Dict[int, Point] = {}
    if not get_points:
        return pairs
    for lp in launch_points:
        nearest = min(get_points, key=lambda gp: math.hypot(lp['x'] - gp['x'], lp['y'] - gp['y']))
        pairs[lp['id']] = nearest
    return pairs


def euclidean_cost(route: List[Point]) -> float:
    cost = 0.0
    for i in range(1, len(route)):
        p1, p2 = route[i - 1], route[i]
        cost += math.hypot(p2['x'] - p1['x'], p2['y'] - p1['y'])
    return cost


def route_has_paths(route: List[Point], connections: Connections) -> bool:
    if len(route) < 2:
        return True
    for i in range(1, len(route)):
        if not bfs_path(connections, route[i - 1]['id'], route[i]['id']):
            return False
    return True


def reorder_stores_2opt(start: Point, end: Point, stores: List[Point], connections: Connections) -> List[Point]:
    if len(stores) < 2:
        return stores
    route = [start] + stores + [end]
    best_cost = euclidean_cost(route)
    improved = True
    while improved:
        improved = False
        for i in range(1, len(route) - 2):
            for k in range(i + 1, len(route) - 1):
                new_route = route[:i] + list(reversed(route[i:k + 1])) + route[k + 1:]
                new_cost = euclidean_cost(new_route)
                if new_cost < best_cost and route_has_paths(new_route, connections):
                    route = new_route
                    best_cost = new_cost
                    improved = True
                    break
            if improved:
                break
    return route[1:-1]


# --- RUNNER ---

def run_simulation_instance(run: int, strategy: Callable, config: Dict) -> Dict:
    log_dir = config.get('log_dir', LOG_DIR)
    big_log_subdir = config.get('big_log_subdir', 'big_logs')
    big_log_every = config.get('big_log_every', 100)
    fuel_price = config.get('fuel_price_base', config.get('fuel_price', FUEL_PRICE))
    seed_val = config.get('seed')
    max_neighbors = config.get('max_neighbors', MAX_NEIGHBORS)
    car_meta = config.get('car_metadata', CAR_METADATA)
    market_file = config.get('market_file', 'marketlist.json')
    static_cost_per_car = config.get('static_cost_per_car', STATIC_COST_PER_CAR)
    extra_km_cost_enabled = config.get('extra_km_cost_enabled', EXTRA_KM_COST_ENABLED)
    extra_km_cost_per_km = config.get('extra_km_cost_per_km', EXTRA_KM_COST_PER_KM)
    entrance_fee_min = config.get('store_entrance_fee_min', STORE_ENTRANCE_FEE_MIN)
    entrance_fee_max = config.get('store_entrance_fee_max', STORE_ENTRANCE_FEE_MAX)
    traffic_jam_prob = config.get('traffic_jam_prob', 0.05)
    traffic_jam_fuel_multiplier = config.get('traffic_jam_fuel_multiplier', 1.5)

    ensure_log_dir(log_dir)
    big_log_dir = os.path.join(log_dir, big_log_subdir)
    ensure_log_dir(big_log_dir)

    points: List[Point] = random_points(config)
    connections = connect_points(points, max_neighbors)

    launch_points = [p for p in points if p['type'] == POINT_LAUNCH]
    stores = [p for p in points if p['type'] == POINT_STORE]
    get_points = [p for p in points if p['type'] == POINT_GET]
    if not launch_points:
        return None

    market_items = _MARKET_ITEMS if _MARKET_ITEMS is not None else load_market_items(market_file)
    assign_store_inventories(stores, market_items, min_item_types=2,
                              entrance_fee_range=(entrance_fee_min, entrance_fee_max))
    store_buckets = assign_stores_to_launches(launch_points, stores)
    launch_to_get = pair_launch_to_getpoints(launch_points, get_points)

    per_car_results = []
    overall_cause = 'Completed all cars'
    total_budget_all = 0.0
    total_distance_all = 0.0
    total_c1_all = 0.0
    total_c2_fuel_all = 0.0
    total_c2_extra_all = 0.0
    total_c3_entrance_all = 0.0
    total_c3_items_all = 0.0

    brand_summary: Dict[str, Dict[str, float | List[int]]] = {}

    for lp in launch_points:
        assigned = store_buckets.get(lp['id'], [])
        get_point = launch_to_get.get(lp['id'], lp)
        # Select covering stores from all stores first (use assigned as draft hint)
        draft_list = shopping_list_from_inventory_pool(assigned, market_items)
        covering_stores = select_covering_stores(stores, draft_list, lp, max_stores=6)
        base_route = covering_stores if covering_stores else assigned
        # Build final shopping list from the actual stores that will be visited
        shopping_list = shopping_list_from_inventory_pool(base_route, market_items)
        original_order = list(base_route)
        reordered = reorder_stores_2opt(lp, get_point, base_route, connections)
        quest_points = [lp] + reordered + [get_point]
        car_task = {
            'launch': lp,
            'get_point': get_point,
            'stores_original': original_order,
            'stores_route': reordered,
            'shopping_list': shopping_list,
            'static_cost_per_car': static_cost_per_car,
            'extra_km_cost_enabled': extra_km_cost_enabled,
            'extra_km_cost_per_km': extra_km_cost_per_km,
            'traffic_jam_prob': traffic_jam_prob,
            'traffic_jam_fuel_multiplier': traffic_jam_fuel_multiplier,
        }
        res = strategy(car_task, points, connections, car_meta, fuel_price=fuel_price)
        per_car_results.append((lp, quest_points, shopping_list, res, original_order, reordered, get_point))
        total_budget_all += res['budget']
        total_distance_all += res['distance']
        total_c1_all += res.get('c1', 0.0)
        total_c2_fuel_all += res.get('c2_fuel', 0.0)
        total_c2_extra_all += res.get('c2_extra', 0.0)
        total_c3_entrance_all += res.get('c3_entrance', 0.0)
        total_c3_items_all += res.get('c3_items', 0.0)
        if res['cause'] != 'Completed quest' and overall_cause == 'Completed all cars':
            overall_cause = f'Car {lp["id"]} failed: {res["cause"]}'

    # Build brand summary after strategies run so all gas station metadata is available
    gas_points = [p for p in points if p['type'] == POINT_GAS]
    for gp in gas_points:
        brand = gp.get('brand', 'Brand ?')
        price = gp.get('fuel_price', fuel_price)
        if brand not in brand_summary:
            brand_summary[brand] = {'price': price, 'stations': []}
        brand_summary[brand]['stations'].append(gp['id'])

    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    target_dir = big_log_dir if big_log_every and run % big_log_every == 0 else log_dir
    log_filename = os.path.join(target_dir, f'log_{timestamp}_run{run}.txt')
    with open(log_filename, 'w', encoding='utf-8') as logf:
        logf.write(f'Run {run} Log\n')
        logf.write(f'Seed: {seed_val}\n')
        logf.write('Fuel Brands:\n')
        for brand, info in sorted(brand_summary.items(), key=lambda kv: kv[0]):
            station_list = ','.join(str(sid) for sid in sorted(info['stations']))
            logf.write(f'  {brand}: price={info["price"]:.2f} THB/L, stations=[{station_list}]\n')
        logf.write('Points:\n')
        for p in points:
            if p['type'] == POINT_GAS:
                price_val = p.get('fuel_price', fuel_price)
                brand_val = p.get('brand', '')
                logf.write(
                    f'  id={p["id"]}, type={p["type"]}, brand={brand_val}, '
                    f'fuel_price={price_val:.2f}, x={p["x"]:.2f}, y={p["y"]:.2f}\n'
                )
            else:
                logf.write(f'  id={p["id"]}, type={p["type"]}, x={p["x"]:.2f}, y={p["y"]:.2f}\n')
        logf.write(f'Overall Cause: {overall_cause}\n')
        logf.write(f'Total distance (all cars): {total_distance_all:.2f} m\n')
        logf.write(f'Total budget (all cars): {total_budget_all:.2f} THB\n')
        logf.write('--- Per-Car Results ---\n')
        for lp, quest, shopping_list, res, original_order, reordered, get_point in per_car_results:
            logf.write(f'Car from Launch {lp["id"]}\n')
            logf.write('  Shopping list (name:qty): ' + ', '.join(f"{item['name']}:{item['qty']}" for item in shopping_list) + '\n')
            logf.write('  Original stores: ' + ' -> '.join(str(p['id']) for p in original_order) + '\n')
            logf.write('  Optimized quest: ' + ' -> '.join(str(p['id']) for p in quest) + '\n')
            logf.write(f'  Get point: {get_point["id"]}\n')
            logf.write(f'  Cause: {res["cause"]}\n')
            logf.write(f'  Distance: {res["distance"]:.2f} m\n')
            logf.write(
                f'  Budget: {res["budget"]:.2f} THB'
                f' [C1={res.get("c1", 0):.2f}'
                f', C2_fuel={res.get("c2_fuel", 0):.2f}'
                f', C2_extra={res.get("c2_extra", 0):.2f}'
                f', C3_entrance={res.get("c3_entrance", 0):.2f}'
                f', C3_items={res.get("c3_items", 0):.2f}]\n'
            )
            logf.write('  Car Log:\n')
            for entry in res['log']:
                logf.write(f'    {entry}\n')
            logf.write('\n')

    # print(f'Run {run}: Overall Cause: {overall_cause} | Budget(all cars): {total_budget_all:.2f}', flush=True)

    # Delete log file if this run failed due to fuel and the flag is set
    if config.get('delete_fuel_fail_logs', False):
        cause_lower = overall_cause.lower()
        fuel_fail = any(kw in cause_lower for kw in ('out of fuel', 'fuel below reserve', 'no gas station', 'no route to gas'))
        if fuel_fail:
            try:
                os.remove(log_filename)
            except OSError:
                pass

    # Delete log file for any unfinished run
    if config.get('delete_failed_logs', False) and overall_cause != 'Completed all cars':
        try:
            os.remove(log_filename)
        except OSError:
            pass

    return {
        'cause': overall_cause,
        'budget': total_budget_all,
        'c1': total_c1_all,
        'c2_fuel': total_c2_fuel_all,
        'c2_extra': total_c2_extra_all,
        'c3_entrance': total_c3_entrance_all,
        'c3_items': total_c3_items_all,
    }


# --- SCENARIO HELPERS FOR VISUALIZATION / COMPARISON ---

def build_scenario(config: Dict, seed: int | None = None) -> Dict:
    """Build a single simulation scenario without running a strategy.

    Returns points, connections, and per-car tasks so multiple strategies can run
    against the exact same map/inventories. If a seed is provided, the random
    stream is restored after generation to avoid perturbing global randomness.
    """

    state = random.getstate()
    if seed is not None:
        random.seed(seed)

    points: List[Point] = random_points(config)
    connections = connect_points(points, config.get('max_neighbors', MAX_NEIGHBORS))

    launch_points = [p for p in points if p['type'] == POINT_LAUNCH]
    stores = [p for p in points if p['type'] == POINT_STORE]
    get_points = [p for p in points if p['type'] == POINT_GET]
    if not launch_points:
        if seed is not None:
            random.setstate(state)
        return {
            'points': points,
            'connections': connections,
            'car_tasks': [],
        }

    market_items = load_market_items(config.get('market_file', 'marketlist.json'))
    assign_store_inventories(stores, market_items, min_item_types=2,
                              entrance_fee_range=(config.get('store_entrance_fee_min', STORE_ENTRANCE_FEE_MIN),
                                                  config.get('store_entrance_fee_max', STORE_ENTRANCE_FEE_MAX)))
    store_buckets = assign_stores_to_launches(launch_points, stores)
    launch_to_get = pair_launch_to_getpoints(launch_points, get_points)

    car_tasks = []
    for lp in launch_points:
        assigned = store_buckets.get(lp['id'], [])
        get_point = launch_to_get.get(lp['id'], lp)
        draft_list = shopping_list_from_inventory_pool(assigned, market_items)
        covering_stores = select_covering_stores(stores, draft_list, lp, max_stores=6)
        base_route = covering_stores if covering_stores else assigned
        shopping_list = shopping_list_from_inventory_pool(base_route, market_items)
        original_order = list(base_route)
        reordered = reorder_stores_2opt(lp, get_point, base_route, connections)
        car_tasks.append({
            'launch': lp,
            'get_point': get_point,
            'stores_original': original_order,
            'stores_route': reordered,
            'shopping_list': shopping_list,
        })

    if seed is not None:
        random.setstate(state)

    return {
        'points': points,
        'connections': connections,
        'car_tasks': car_tasks,
    }


def run_strategy_on_scenario(strategy: Callable, scenario: Dict, config: Dict) -> Dict:
    """Run one strategy on a prebuilt scenario and return rich results for viz."""

    fuel_price = config.get('fuel_price_base', config.get('fuel_price', FUEL_PRICE))
    car_meta = config.get('car_metadata', CAR_METADATA)

    points = copy.deepcopy(scenario['points'])
    connections = scenario['connections']
    car_tasks = scenario['car_tasks']

    per_car_results = []
    overall_cause = 'Completed all cars'
    total_budget_all = 0.0
    total_distance_all = 0.0
    total_c1_all = 0.0
    total_c2_fuel_all = 0.0
    total_c2_extra_all = 0.0
    total_c3_entrance_all = 0.0
    total_c3_items_all = 0.0

    for task in car_tasks:
        res = strategy(task, points, connections, car_meta, fuel_price=fuel_price)
        quest_points = [task['launch']] + (task.get('stores_route') or task.get('stores_original') or []) + [task['get_point']]
        per_car_results.append({
            'launch_id': task['launch']['id'],
            'quest_route_ids': [p['id'] for p in quest_points],
            'cause': res.get('cause', ''),
            'distance': res.get('distance', 0.0),
            'budget': res.get('budget', 0.0),
            'c1': res.get('c1', 0.0),
            'c2_fuel': res.get('c2_fuel', 0.0),
            'c2_extra': res.get('c2_extra', 0.0),
            'c3_entrance': res.get('c3_entrance', 0.0),
            'c3_items': res.get('c3_items', 0.0),
            'log': res.get('log', []),
        })
        total_budget_all += res.get('budget', 0.0)
        total_distance_all += res.get('distance', 0.0)
        total_c1_all += res.get('c1', 0.0)
        total_c2_fuel_all += res.get('c2_fuel', 0.0)
        total_c2_extra_all += res.get('c2_extra', 0.0)
        total_c3_entrance_all += res.get('c3_entrance', 0.0)
        total_c3_items_all += res.get('c3_items', 0.0)
        if res.get('cause') != 'Completed quest' and overall_cause == 'Completed all cars':
            overall_cause = f'Car {task["launch"]["id"]} failed: {res.get("cause", "")}'

    return {
        'cause': overall_cause,
        'budget': total_budget_all,
        'distance': total_distance_all,
        'c1': total_c1_all,
        'c2_fuel': total_c2_fuel_all,
        'c2_extra': total_c2_extra_all,
        'c3_entrance': total_c3_entrance_all,
        'c3_items': total_c3_items_all,
        'per_car': per_car_results,
        'points': points,
        'connections': connections,
    }


def make_runner(strategy: Callable, config: Dict) -> Callable:
    return partial(run_simulation_instance, strategy=strategy, config=config)


def summarize_in_memory(results: List[Dict]) -> Dict[str, float | int]:
    success = 0
    fail = 0
    total_cost = 0.0
    total_c1 = 0.0
    total_c2_fuel = 0.0
    total_c2_extra = 0.0
    total_c3_entrance = 0.0
    total_c3_items = 0.0
    cost_count = 0
    for res in results:
        if not res:
            fail += 1
            continue
        cause = res.get('cause', '')
        cost = res.get('budget', 0.0)
        if cause and 'Completed all cars' in cause:
            success += 1
            total_cost += cost
            total_c1 += res.get('c1', 0.0)
            total_c2_fuel += res.get('c2_fuel', 0.0)
            total_c2_extra += res.get('c2_extra', 0.0)
            total_c3_entrance += res.get('c3_entrance', 0.0)
            total_c3_items += res.get('c3_items', 0.0)
            cost_count += 1
        else:
            fail += 1
    avg_cost = (total_cost / cost_count) if cost_count else 0.0
    avg_c1 = (total_c1 / cost_count) if cost_count else 0.0
    avg_c2_fuel = (total_c2_fuel / cost_count) if cost_count else 0.0
    avg_c2_extra = (total_c2_extra / cost_count) if cost_count else 0.0
    avg_c3_entrance = (total_c3_entrance / cost_count) if cost_count else 0.0
    avg_c3_items = (total_c3_items / cost_count) if cost_count else 0.0
    total_runs = success + fail
    finish_pct = (success / total_runs) if total_runs else 0.0
    # print('\n--- Simulation Summary ---')
    # print(f'Successful runs: {success}')
    # print(f'Unsuccessful runs: {fail}')
    # print(f'Finish rate: {finish_pct:.2%}')
    # print(f'Average cost (successful runs): {avg_cost:.2f} THB')
    return {
        'success': success,
        'fail': fail,
        'finish_rate': finish_pct,
        'average_cost': avg_cost,
        'avg_c1': avg_c1,
        'avg_c2_fuel': avg_c2_fuel,
        'avg_c2_extra': avg_c2_extra,
        'avg_c3_entrance': avg_c3_entrance,
        'avg_c3_items': avg_c3_items,
    }


# Module-level cache populated in each worker via pool initializer
_MARKET_ITEMS: list | None = None


def _worker_init(market_items: list) -> None:
    """Pool initializer: store market items in each worker process once."""
    global _MARKET_ITEMS
    _MARKET_ITEMS = market_items


def run_many(num_runs: int, strategy: Callable, summarizer: Callable, config: Dict,
             clear_logs: bool = True) -> Dict | None:
    log_dir = config.get('log_dir', LOG_DIR)
    ensure_log_dir(log_dir)
    if clear_logs:
        clear_old_logs(log_dir)

    # Load market items once in the main process; share via initializer (no per-task pickling)
    config = dict(config)
    config.pop('_market_items_cache', None)
    market_items = load_market_items(config.get('market_file', 'marketlist.json'))

    runner = make_runner(strategy, config)
    processes = min(num_runs, multiprocessing.cpu_count())
    chunksize = max(1, num_runs // (processes * 4))
    with multiprocessing.Pool(
        processes=processes,
        initializer=_worker_init,
        initargs=(market_items,),
    ) as pool:
        results = pool.map(runner, range(1, num_runs + 1), chunksize=chunksize)

    return summarizer(results)