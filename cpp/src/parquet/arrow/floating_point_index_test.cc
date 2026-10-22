// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include <gtest/gtest.h>
#include <array>
#include "arrow/scalar.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/util/ubsan.h"
#include "parquet/arrow/reader.h"
#include "parquet/file_reader.h"
#include "parquet/page_index.h"
#include "parquet/statistics.h"
#include "parquet/test_util.h"

namespace parquet::arrow {
TEST(ParquetPageIndex, FloatingPointOrdersInterop) {
  auto reader = ParquetFileReader::OpenFile(
      test::get_data_file("floating_orders_nan_count.parquet"));
  auto metadata = reader->metadata();
  ASSERT_EQ(6, metadata->num_columns());
  ASSERT_EQ(5, metadata->num_row_groups());

  for (int column = 0; column < metadata->num_columns(); ++column) {
    const auto expected_order = column % 2 == 0 ? ColumnOrder::IEEE_754_TOTAL_ORDER
                                                : ColumnOrder::TYPE_DEFINED_ORDER;
    ASSERT_EQ(expected_order,
              metadata->schema()->Column(column)->column_order().get_order());
  }

  constexpr int kAllNaNRowGroup = 2;
  constexpr int64_t kNaNCount = 10;
  // FLOAT, DOUBLE, and FLOAT16 -qNaN and +qNaN bounds.
  const std::array<uint64_t, 3> expected_min_bits{0xffffffff, 0xffffffffffffffff, 0xffff};
  const std::array<uint64_t, 3> expected_max_bits{0x7fffffff, 0x7fffffffffffffff, 0x7fff};
  auto scalar_bits = [](const ::arrow::Scalar& scalar) -> uint64_t {
    switch (scalar.type->id()) {
      case ::arrow::Type::FLOAT:
        return ::arrow::util::SafeCopy<uint32_t>(
            static_cast<const ::arrow::FloatScalar&>(scalar).value);
      case ::arrow::Type::DOUBLE:
        return ::arrow::util::SafeCopy<uint64_t>(
            static_cast<const ::arrow::DoubleScalar&>(scalar).value);
      case ::arrow::Type::HALF_FLOAT:
        return static_cast<const ::arrow::HalfFloatScalar&>(scalar).value;
      default:
        throw ParquetException("Unexpected floating-point scalar type");
    }
  };
  auto page_index_reader = reader->GetPageIndexReader();
  ASSERT_NE(nullptr, page_index_reader);
  auto row_group_index = page_index_reader->RowGroup(kAllNaNRowGroup);
  ASSERT_NE(nullptr, row_group_index);
  auto row_group = metadata->RowGroup(kAllNaNRowGroup);
  for (int column = 0; column < metadata->num_columns(); ++column) {
    SCOPED_TRACE(::testing::Message() << "column=" << column);
    auto statistics = row_group->ColumnChunk(column)->statistics();
    ASSERT_NE(nullptr, statistics);
    ASSERT_EQ(kNaNCount, statistics->nan_count());
    if (column % 2 != 0) {
      ASSERT_FALSE(statistics->HasMinMax());
      ASSERT_EQ(nullptr, row_group_index->GetColumnIndex(column));
      continue;
    }
    ASSERT_TRUE(statistics->HasMinMax());
    auto column_index = row_group_index->GetColumnIndex(column);
    ASSERT_NE(nullptr, column_index);
    auto nan_counts = column_index->nan_counts();
    ASSERT_TRUE(nan_counts.has_value());
    ASSERT_EQ(1U, nan_counts->size());
    EXPECT_EQ(kNaNCount, (*nan_counts)[0]);

    std::shared_ptr<::arrow::Scalar> min;
    std::shared_ptr<::arrow::Scalar> max;
    ASSERT_OK(StatisticsAsScalars(*statistics, &min, &max));
    ASSERT_EQ(expected_min_bits[column / 2], scalar_bits(*min));
    ASSERT_EQ(expected_max_bits[column / 2], scalar_bits(*max));
  }
}

}  // namespace parquet::arrow
