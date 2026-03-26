import json
from typing import Any, Dict, List, Optional

import old_algorithm
import our_alogorithm
from simulation_core import (
    DEFAULT_CONFIG,
    build_scenario,
    run_strategy_on_scenario,
)

AlgorithmResult = Dict[str, Any]


def _merge_config(overrides: Optional[Dict]) -> Dict:
    config = DEFAULT_CONFIG.copy()
    if overrides:
        config.update(overrides)
    return config


def _serializable_points(points: List[Dict]) -> List[Dict]:
    return [
        {
            'id': p['id'],
            'x': p['x'],
            'y': p['y'],
            'type': p.get('type', ''),
        }
        for p in points
    ]


def _serializable_connections(connections: Dict[int, set]) -> Dict[str, List[int]]:
    return {str(k): sorted(list(v)) for k, v in connections.items()}


def _strip_logs(per_car: List[Dict], include_logs: bool) -> List[Dict]:
    out: List[Dict] = []
    for car in per_car:
        entry = {
            'launch_id': car.get('launch_id'),
            'quest_route_ids': car.get('quest_route_ids', []),
            'cause': car.get('cause', ''),
            'distance': car.get('distance', 0.0),
            'budget': car.get('budget', 0.0),
            'c1': car.get('c1', 0.0),
            'c2_fuel': car.get('c2_fuel', 0.0),
            'c2_extra': car.get('c2_extra', 0.0),
            'c3_entrance': car.get('c3_entrance', 0.0),
            'c3_items': car.get('c3_items', 0.0),
        }
        if include_logs:
            entry['log'] = car.get('log', [])
        out.append(entry)
    return out


def _pack_result(raw: AlgorithmResult, include_logs: bool) -> AlgorithmResult:
    return {
        'cause': raw.get('cause', ''),
        'budget': raw.get('budget', 0.0),
        'distance': raw.get('distance', 0.0),
        'c1': raw.get('c1', 0.0),
        'c2_fuel': raw.get('c2_fuel', 0.0),
        'c2_extra': raw.get('c2_extra', 0.0),
        'c3_entrance': raw.get('c3_entrance', 0.0),
        'c3_items': raw.get('c3_items', 0.0),
        'per_car': _strip_logs(raw.get('per_car', []), include_logs),
    }


def build_comparison(seed: Optional[int] = None, config_overrides: Optional[Dict] = None,
                     include_logs: bool = False) -> Dict[str, Any]:
    """
    Build one scenario, run both algorithms on it, and return a JSON-friendly payload.
    """

    config = _merge_config(config_overrides)
    scenario = build_scenario(config, seed=seed)
    if not scenario.get('car_tasks'):
        return {'error': 'Scenario has no launch points; adjust config to generate launches.'}

    our_raw = run_strategy_on_scenario(our_alogorithm.simulate_car_pdf, scenario, config)
    old_raw = run_strategy_on_scenario(old_algorithm.simulate_car_threshold, scenario, config)

    payload = {
        'seed': seed,
        'config': config,
        'points': _serializable_points(scenario['points']),
        'connections': _serializable_connections(scenario['connections']),
        'algorithms': {
            'our': _pack_result(our_raw, include_logs),
            'old': _pack_result(old_raw, include_logs),
        },
    }
    return payload


def build_comparison_json(seed: Optional[int] = None, config_overrides: Optional[Dict] = None,
                          include_logs: bool = False, indent: int = 2) -> str:
    return json.dumps(build_comparison(seed, config_overrides, include_logs), indent=indent)
