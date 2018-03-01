<?php
/**
 * @author Sujith Haridasan <sharidasan@owncloud.com>
 *
 * @copyright Copyright (c) 2018, ownCloud GmbH
 * @license AGPL-3.0
 *
 * This code is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License, version 3,
 * along with this program.  If not, see <http://www.gnu.org/licenses/>
 *
 */

namespace OC\Repair;


use OCP\DB\QueryBuilder\IQueryBuilder;
use OCP\IDBConnection;
use OCP\Migration\IOutput;
use OCP\Migration\IRepairStep;

class RepairOrphanedSubshare implements IRepairStep {

	/** @var IDBConnection  */
	private $connection;

	/** @var  IQueryBuilder */
	private $availableParents;

	/** @var  IQueryBuilder */
	private $getSharesWithParentsAvailable;

	/** @var  IQueryBuilder */
	private $deleteOrphanReshares;
	/**
	 * RepairOrphanedSubshare constructor.
	 *
	 * @param IDBConnection $connection
	 */
	public function __construct(IDBConnection $connection) {
		$this->connection = $connection;
	}

	/**
	 * Returns the step's name
	 *
	 * @return string
	 * @since 9.1.0
	 */
	public function getName() {
		return 'Repair orphaned reshare';
	}

	/**
	 * This delete query deletes orphan shares whose parents are missing
	 * @param $parentId
	 */
	private function setDeleteOrphanSharesQuery($parentId) {
		$builder = $this->connection->getQueryBuilder();
		$builder
			->delete('share')
			->where($builder->expr()->eq('parent', $builder->createNamedParameter($parentId)));
		$this->deleteOrphanReshares = $builder;
	}

	/**
	 * This select query is to get the result of parents === id
	 * @param $parentId
	 */
	private function setSelectShareId($parentId) {
		$builder = $this->connection->getQueryBuilder();
		$builder
			->select('id')
			->from('share')
			->where($builder->expr()->eq('id', $builder->createNamedParameter($parentId)));//Where id = parent from above query or available parents
		$this->getSharesWithParentsAvailable = $builder;
	}

	/**
	 * This select query gives us unique parents from the table
	 * @param $pageLimit
	 * @param $paginationOffset
	 */
	private function setSelectGetAllParents($pageLimit, $paginationOffset) {
		$builder = $this->connection->getQueryBuilder();
		$builder
			->select('parent')
			->from('share')
			->groupBy('parent')->orderBy('parent')->setMaxResults($pageLimit)->setFirstResult($paginationOffset);

		$this->availableParents = $builder;
	}
	/**
	 * Run repair step.
	 * Must throw exception on error.
	 *
	 * @param IOutput $output
	 * @throws \Exception in case of failure
	 * @since 9.1.0
	 */
	public function run(IOutput $output) {
		$missingParents = [];
		$paginationOffset = 0;
		$pageLimit = 1000;
		$deleteRows = [];

		do {
			$this->setSelectGetAllParents($pageLimit, $paginationOffset);
			$results = $this->availableParents->execute();
			$rows = $results->fetchAll();
			echo "\nAll Parents \n";
			var_dump($rows);
			$results->closeCursor();
			$paginationOffset += $pageLimit;
			$lastResultCount = 0;

			foreach ($rows as $row) {
				echo "\nSo the parent looking for: ". $row['parent'] . " Available parents\n";
				if ($row['parent'] === null) {
					$lastResultCount++;
					continue;
				}
				$this->setSelectShareId($row['parent']);
				$getIdQuery = $this->getSharesWithParentsAvailable->execute();
				$getIdRows = $getIdQuery->fetchAll();
				var_dump($getIdRows);
				$getIdQuery->closeCursor();
				$lastResultCount++;
				/**
				 * Check if the query result is empty.
				 * And if parent is not added to missingParents array, then
				 * add it so that shares of this parent who became orphans can
				 * be deleted later.
				 */
				if ((count($getIdRows) === 0) &&
					(in_array($row['parent'], $missingParents, true) === false)) {
					$missingParents[] = $row['parent'];
				}
			}

			if (count($missingParents) > 0) {
				foreach ($missingParents as $missingParent) {
					$this->setDeleteOrphanSharesQuery($missingParent);
					$deleteRows[] = $this->deleteOrphanReshares;
				}
				$missingParents = []; //Reset the array
			}
		} while($lastResultCount > 0);

		if (count($deleteRows) > 0) {
			foreach ($deleteRows as $deleteRow) {
				$deleteRow->execute();
			}
		}
	}
}