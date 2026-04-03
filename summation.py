import our_alogorithm
import old_algorithm
import matplotlib.pyplot as plt
from itertools import count
from simulation_core import(
	MAX_NEIGHBORS,
	connect_points
)

if __name__ == '__main__':
	our_summary = our_alogorithm.main()
	print("run our algo..... successful!")
	our_rate = our_summary.get('finish_rate') if our_summary else None
	our_cost = our_summary.get('average_cost') if our_summary else None
	storex = our_summary.get("storex") if our_summary else None
	storey = our_summary.get("storey") if our_summary else None
	getx = our_summary.get("getx") if our_summary else None
	gety = our_summary.get("gety") if our_summary else None
	gasx = our_summary.get("gasx") if our_summary else None
	gasy = our_summary.get("gasy") if our_summary else None
	idd = count(0)
	pointss = [{'id': next(idd),'x':x,'y':y} for (x,y) in zip(storex+getx+gasx,storey+gety+gasy)]
	pointsx = []
	pointsy = []
	for _ in range(len(pointss)):
		pointsx.append(pointss[_]["x"])
		pointsy.append(pointss[_]["y"])
	connect = connect_points(pointss,MAX_NEIGHBORS)
	for node, connections in connect.items():
		if node < len(pointsx) and node < len(pointsy):
			for connected_node in connections:
				if isinstance(connected_node, int) and connected_node < len(pointsx):
					plt.plot([pointsx[node], pointsx[connected_node]], [pointsy[node], pointsy[connected_node]], 'k-', linewidth = "0.8")
	plt.scatter(storex,storey,label = "Store")
	plt.scatter(gasx,gasy,label = "Gas Station")
	plt.scatter(getx,gety,label = "Get Point")
	print('--- our_alogorithm ---')
	print(f'Finish rate: {our_rate:.2%}')
	print(f'Avg Cost: {our_cost:.2f} THB')
	plt.legend(loc = "center right")
	plt.grid()
	plt.xlabel('X-Level')
	plt.ylabel('Y-Level')
	
	# Show on low map size
	# plt.xlim(-5, 121)
	# plt.ylim(-1, 101)

	# Show on high map size
	plt.xlim(-500, 12100)
	plt.ylim(-100, 10100)
	
	plt.show()
	plt.close('all')

	old_summary = old_algorithm.main()
	print("run old algo..... successful!")
	old_rate = old_summary.get('finish_rate') if old_summary else None
	old_cost = old_summary.get('average_cost') if old_summary else None
	old_x = old_summary.get("ganx") if old_summary else None
	old_y = old_summary.get("gany") if old_summary else None
	if old_cost in (None, 0):
		evalu = 0.0
	else:
		evalu = (abs(our_cost - old_cost) / old_cost)
	print('--- old_algorithm ---')
	print(f'Finish rate: {old_rate:.2%}')
	print(f'Avg Cost: {old_cost:.2f} THB')
	print("---------------------")
	print(f"Diff: {evalu:.2%}")