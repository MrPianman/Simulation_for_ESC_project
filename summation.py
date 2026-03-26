import our_alogorithm
import old_algorithm

if __name__ == '__main__':
	our_summary = our_alogorithm.main()
	print("run our algo..... successful!")
	old_summary = old_algorithm.main()
	print("run old algo..... successful!")
	our_rate = our_summary.get('finish_rate') if our_summary else None
	our_cost = our_summary.get('average_cost') if our_summary else None
	old_rate = old_summary.get('finish_rate') if old_summary else None
	old_cost = old_summary.get('average_cost') if old_summary else None
	if old_cost in (None, 0):
		evalu = 0.0
	else:
		evalu = (abs(our_cost - old_cost) / old_cost)
	print('--- our_alogorithm ---')
	print(f'Finish rate: {our_rate:.2%}')
	print(f'Avg Cost: {our_cost:.2f} THB')
	print('--- old_algorithm ---')
	print(f'Finish rate: {old_rate:.2%}')
	print(f'Avg Cost: {old_cost:.2f} THB')
	print("---------------------")
	print(f"Diff: {evalu:.2%}")
