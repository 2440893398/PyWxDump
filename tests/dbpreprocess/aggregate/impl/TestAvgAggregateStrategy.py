import unittest
from unittest.mock import patch, MagicMock

from sqlalchemy import Row
from sqlglot import parse_one
from sqlglot.expressions import Select, Sum, Count, Avg, With, Distinct

from pywxdump.dbpreprocess.aggregate.MutableRow import MutableRow
from pywxdump.dbpreprocess.aggregate.impl.avg_aggregate_strategy import AvgAggregateStrategy


class TestAvgAggregateStrategy(unittest.TestCase):

    @patch.object(AvgAggregateStrategy, '__init__', return_value=None)
    def setUp(self, mock_init):
        self.strategy = AvgAggregateStrategy(None, None)

    def test_sqlPreprocessing_basic_avg(self):
        sql = "SELECT AVG(salary) FROM employees"
        tree = parse_one(sql)
        self.strategy.aggregateField = tree.find(Avg)
        processed_tree = self.strategy.sql_preprocessing(tree)
        select = processed_tree.find(Select)

        self.assertEqual(len(select.expressions), 3)
        self.assertIsInstance(select.expressions[1], Sum)
        self.assertIsInstance(select.expressions[2], Count)
        self.assertEqual(select.expressions[1].alias, "__sum_salary")
        self.assertEqual(select.expressions[2].alias, "__count_salary")

    def test_sqlPreprocessing_avg_with_alias(self):
        sql = "SELECT AVG(salary) AS average_salary FROM employees"
        tree = parse_one(sql)
        self.strategy.aggregateField = tree.find(Select).expressions[0]
        processed_tree = self.strategy.sql_preprocessing(tree)
        select = processed_tree.find(Select)

        self.assertEqual(len(select.expressions), 3)
        self.assertEqual(select.expressions[1].alias, "__sum_average_salary")
        self.assertEqual(select.expressions[2].alias, "__count_average_salary")

    def test_sqlPreprocessing_multiple_avg(self):
        sql = "SELECT AVG(salary), AVG(age) FROM employees"
        tree = parse_one(sql)
        self.strategy.aggregateField = tree.find(Select).expressions[0]
        processed_tree = self.strategy.sql_preprocessing(tree)
        select = processed_tree.find(Select)

        self.strategy.aggregateField = tree.find(Select).expressions[1]
        select = self.strategy.sql_preprocessing(select)
        select = select.find(Select)

        self.assertEqual(len(select.expressions), 6)
        self.assertIsInstance(select.expressions[2], Sum)
        self.assertIsInstance(select.expressions[3], Count)
        self.assertIsInstance(select.expressions[4], Sum)
        self.assertIsInstance(select.expressions[5], Count)

    def test_sqlPreprocessing_avg_with_other_fields(self):
        sql = "SELECT name, AVG(salary), department FROM employees"
        tree = parse_one(sql)
        self.strategy.aggregateField = tree.find(Avg)
        processed_tree = self.strategy.sql_preprocessing(tree)
        select = processed_tree.find(Select)

        self.assertEqual(len(select.expressions), 5)
        self.assertIsInstance(select.expressions[3], Sum)
        self.assertIsInstance(select.expressions[4], Count)

    def test_sqlPreprocessing_avg_with_group_by(self):
        sql = "SELECT department, AVG(salary) FROM employees GROUP BY department"
        tree = parse_one(sql)
        self.strategy.aggregateField = tree.find(Avg)
        processed_tree = self.strategy.sql_preprocessing(tree)
        select = processed_tree.find(Select)

        self.assertEqual(len(select.expressions), 4)
        self.assertIsInstance(select.expressions[2], Sum)
        self.assertIsInstance(select.expressions[3], Count)

    def test_sqlPreprocessing_avg_with_having(self):
        sql = "SELECT department, AVG(salary) FROM employees GROUP BY department HAVING AVG(salary) > 50000"
        tree = parse_one(sql)
        self.strategy.aggregateField = tree.find(Avg)
        processed_tree = self.strategy.sql_preprocessing(tree)
        select = processed_tree.find(Select)

        self.assertEqual(len(select.expressions), 4)
        self.assertIsInstance(select.expressions[2], Sum)
        self.assertIsInstance(select.expressions[3], Count)

    def test_sqlPreprocessing_avg_with_order_by(self):
        sql = "SELECT department, AVG(salary) FROM employees GROUP BY department ORDER BY AVG(salary)"
        tree = parse_one(sql)
        self.strategy.aggregateField = tree.find(Avg)
        processed_tree = self.strategy.sql_preprocessing(tree)
        select = processed_tree.find(Select)

        self.assertEqual(len(select.expressions), 4)
        self.assertIsInstance(select.expressions[2], Sum)
        self.assertIsInstance(select.expressions[3], Count)

    def test_sqlPreprocessing_avg_with_distinct(self):
        sql = "SELECT AVG(DISTINCT salary) FROM employees"
        tree = parse_one(sql)
        self.strategy.aggregateField = tree.find(Avg)
        processed_tree = self.strategy.sql_preprocessing(tree)
        select = processed_tree.find(Select)

        self.assertEqual(len(select.expressions), 3)
        self.assertIsInstance(select.expressions[1], Sum)
        self.assertIsInstance(select.expressions[2], Count)
        self.assertTrue(select.expressions[1].find(Distinct))
        self.assertTrue(select.expressions[2].find(Distinct))

    def test_sqlPreprocessing_avg_with_case(self):
        sql = "SELECT AVG(CASE WHEN department = 'IT' THEN salary ELSE 0 END) FROM employees"
        tree = parse_one(sql)
        self.strategy.aggregateField = tree.find(Avg)
        processed_tree = self.strategy.sql_preprocessing(tree)
        select = processed_tree.find(Select)

        self.assertEqual(len(select.expressions), 3)
        self.assertIsInstance(select.expressions[1], Sum)
        self.assertIsInstance(select.expressions[2], Count)

    def test_sqlPreprocessing_avg_with_function(self):
        sql = "SELECT AVG(COALESCE(salary, 0)) FROM employees"
        tree = parse_one(sql)
        self.strategy.aggregateField = tree.find(Avg)
        processed_tree = self.strategy.sql_preprocessing(tree)
        select = processed_tree.find(Select)

        self.assertEqual(len(select.expressions), 3)
        self.assertIsInstance(select.expressions[1], Sum)
        self.assertIsInstance(select.expressions[2], Count)

    def test_sqlPreprocessing_avg_with_expression(self):
        sql = "SELECT AVG(salary + bonus) FROM employees"
        tree = parse_one(sql)
        self.strategy.aggregateField = tree.find(Avg)
        processed_tree = self.strategy.sql_preprocessing(tree)
        select = processed_tree.find(Select)

        self.assertEqual(len(select.expressions), 3)
        self.assertIsInstance(select.expressions[1], Sum)
        self.assertIsInstance(select.expressions[2], Count)

    def test_sqlPreprocessing_no_avg(self):
        sql = "SELECT name, salary FROM employees"
        tree = parse_one(sql)
        self.strategy.aggregateField = tree.find(Avg)
        processed_tree = self.strategy.sql_preprocessing(tree)
        select = processed_tree.find(Select)

        self.assertEqual(len(select.expressions), 2)
        self.assertFalse(any(isinstance(expr, Sum) for expr in select.expressions))
        self.assertFalse(any(isinstance(expr, Count) for expr in select.expressions))

    # TODO - Fix this test
    # def test_sqlPreprocessing_avg_in_complex_query(self):
    #     sql = """
    #     WITH dept_avg AS (
    #         SELECT department, AVG(salary) AS dept_avg_salary
    #         FROM employees
    #         GROUP BY department
    #     )
    #     SELECT e.name, e.salary, d.dept_avg_salary,
    #            AVG(e.salary) OVER (PARTITION BY e.department) AS running_avg
    #     FROM employees e
    #     JOIN dept_avg d ON e.department = d.department
    #     WHERE e.salary > (SELECT AVG(salary) FROM employees)
    #     """
    #     tree = parse_one(sql)
    #     self.strategy.aggregateField = tree.find(Avg)
    #     processed_tree = self.strategy.sqlPreprocessing(tree)
    #
    #     # Check CTE
    #     cte = processed_tree.find(With).expressions[0].this
    #     self.assertEqual(len(cte.expressions), 4)
    #     self.assertIsInstance(cte.expressions[2], Sum)
    #     self.assertIsInstance(cte.expressions[3], Count)
    #
    #     # Check main query
    #     main_select = processed_tree.find(Select)
    #     self.assertEqual(len(main_select.expressions), 6)
    #     self.assertIsInstance(main_select.expressions[4], Sum)
    #     self.assertIsInstance(main_select.expressions[5], Count)
    #
    #     # Check subquery in WHERE clause
    #     subquery = main_select.where.this.right.this
    #     self.assertEqual(len(subquery.expressions), 3)
    #     self.assertIsInstance(subquery.expressions[1], Sum)
    #     self.assertIsInstance(subquery.expressions[2], Count)

    # TODO - Fix this test
    # def test_sqlPreprocessing_avg_in_subquery(self):
    #     sql = "SELECT * FROM (SELECT AVG(salary) FROM employees) AS subquery"
    #     tree = parse_one(sql)
    #     self.strategy.aggregateField = tree.find(Avg)
    #     processed_tree = self.strategy.sqlPreprocessing(tree)
    #     subquery = processed_tree.find(Select).from_.this.find(Select)
    #
    #     self.assertEqual(len(subquery.expressions), 3)
    #     self.assertIsInstance(subquery.expressions[1], Sum)
    #     self.assertIsInstance(subquery.expressions[2], Count)

    def create_row(self, data):
        mock_parent = MagicMock()
        mock_key_to_index = {f'column_{i}': i for i in range(len(data))}
        return Row(mock_parent, None, mock_key_to_index, data)

    def test_aggregate_existing_key(self):
        self.strategy.groupFieldIndex = [0]  # Assume grouping by the first column
        self.strategy.aggregateFieldIndex = 1
        self.strategy.sum_field_index = 2
        self.strategy.count_field_index = 3

        initial_row = self.create_row([1, 50, 100, 2])
        aggregated_results = MutableRow(initial_row)
        row = self.create_row([1, 0, 50, 1])  # [group_key, avg, sum, count]

        self.strategy.aggregate(aggregated_results, row)

        self.assertEqual(aggregated_results[0], 1)
        self.assertEqual(aggregated_results[1], 50)  # (100 + 50) / (2 + 1)
        self.assertEqual(aggregated_results[2], 150)
        self.assertEqual(aggregated_results[3], 3)

    def test_aggregate_zero_count(self):
        self.strategy.groupFieldIndex = [0]  # Assume grouping by the first column
        self.strategy.aggregateFieldIndex = 1
        self.strategy.sum_field_index = 2
        self.strategy.count_field_index = 3

        aggregated_results = MutableRow(self.create_row([1, 0, 0, 0]))
        row = self.create_row([1, 0, 0, 0])  # [group_key, avg, sum, count]
        # 抛出异常
        with self.assertRaises(ValueError):
            self.strategy.aggregate(aggregated_results, row)

if __name__ == '__main__':
    unittest.main()
