package spendreport;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class LambdaExample {
    static class Visit {
        int userId;
        String url;

        public Visit(int userId, String url) {
            this.userId = userId;
            this.url = url;
        }

        public int getUserId() {
            return userId;
        }
    }

    public static void main(String[] args) {
        List<Visit> visits = new ArrayList<>();
        visits.add(new Visit(1, "https://example.com"));
        visits.add(new Visit(2, "https://example.com"));
        visits.add(new Visit(3, "https://example.com"));

        // 使用 lambda 表达式提取所有访问记录的用户 ID
        List<Integer> userIds = visits.stream()
                .map(visit -> visit.userId)
                .collect(Collectors.toList());

        System.out.println(userIds); // 输出：[1, 2, 3]
    }
}
