package integritymigration.integritymigration;

import com.in6k.dto.core.Period;
import com.in6k.dto.reports.ActivitySummaryByPeriod;
import com.in6k.dto.requests.ActivityByPeriodRequest;
import org.apache.commons.lang.time.DateUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.dao.DataAccessException;
import org.springframework.data.elasticsearch.core.ElasticsearchTemplate;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.web.client.RestTemplate;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.*;
import java.util.*;

@SpringBootApplication
public class IntegrityMigrationApplication implements CommandLineRunner {

    public static void main(String[] args) {
        SpringApplication.run(IntegrityMigrationApplication.class, args);
    }

    @Autowired
    JdbcTemplate jdbcTemplate;

    private RestTemplate restTemplate = new RestTemplate();
    final String depositoryUrl = "http://127.0.0.1:8082/";
    final static String OLD_DB_SHEMA_NAME = "testing_dump";


    @Override
    public void run(String... strings) throws Exception {
        List<Long> users = getNotMergedUserIds();
        LocalDateTime localDateTime = LocalDateTime.of(LocalDate.of(2015, Month.JANUARY, 1), LocalTime.of(0, 0));
        while (!localDateTime.toLocalDate().equals(LocalDate.now())) {
            System.out.println(localDateTime.toString());
            for (Long userId : users) {
                ZonedDateTime zonedTime = ZonedDateTime.of(localDateTime, ZoneId.of("UTC"));
                Long total = 0L;
                Long domainId = 0L;
                Long oldTotal = 0L;
                Integer offset = 0;

                try {
                    domainId = getUserDomainId(userId);
                    offset = getOffset(userId, zonedTime);
                    ActivityByPeriodRequest request = setupRequest(domainId, userId, Date.from(zonedTime.toInstant()), offset);

                    oldTotal = getSumActivityInOldDB(userId, zonedTime, offset);

                    total = Arrays.stream(restTemplate.postForObject(
                            depositoryUrl + "get_activity_summary_by_period",
                            request,
                            ActivitySummaryByPeriod[].class))
                            .mapToLong(ActivitySummaryByPeriod::getTotal)
                            .sum();
                    total = total / 60;

                    if (!total.equals(oldTotal)) {
                        String sql = "INSERT INTO bad_activities_peer_day " +
                                "(user_id, date, old_db_time, new_db_time, domain_id, offset) VALUES (?,?, ?, ?, ?, ?)";
                        jdbcTemplate.update(sql, userId, Date.from(zonedTime.toInstant()), oldTotal, total, domainId, offset);
                    }

                } catch (Exception e) {
                    String sql = "INSERT INTO bad_activities_peer_day " +
                            "(user_id, date, old_db_time, new_db_time, domain_id, offset, exeption) VALUES (?,?, ?, ?, ?, ?, ?)";
                    jdbcTemplate.update(sql, userId, Date.from(zonedTime.toInstant()), oldTotal, total, domainId, offset, e.toString());
                }
            }

            localDateTime = localDateTime.plusDays(1L);
        }
    }

    ActivityByPeriodRequest setupRequest(Long domainId, Long userId, Date date, Integer offset) {
        ActivityByPeriodRequest request = new ActivityByPeriodRequest();

        Date to = date;
        Period period = new Period();
        period.setFromDate(date);

        to = DateUtils.addSeconds(DateUtils.addMinutes(DateUtils.addHours(to, 23), 59), 59);
        period.setEnd(to);

        request.period = period;
        request.domainId = domainId;
        request.timeZone = TimeZone.getTimeZone(Arrays.asList(TimeZone.getAvailableIDs(offset * 1000)).get(1));
        request.userIds = Collections.singletonList(userId);
        request.isFilterRequest = true;

        return request;
    }

    Long getUserDomainId(Long id) {
        String sql = "SELECT d.domain_id\n" +
                "FROM domain_to_users d\n" +
                "  JOIN users u ON u.id = d.user_id\n" +
                "WHERE u.id = ? LIMIT 1";
        return jdbcTemplate.queryForObject(sql, new Object[]{id}, Long.class);
    }

    Long getSumActivityInOldDB(Long userId, ZonedDateTime zonedDateTime, Integer offset) {

        String sql = "SELECT\n" +
                "                        sum(online) + sum(offline)\n" +
                "                      FROM " + OLD_DB_SHEMA_NAME + ".activity_summaries\n" +
                "                      WHERE user_id = ? AND activity_day = ?\n" +
                "                            AND offset = ?\n" +
                "                      GROUP BY offset";

        try {
            return jdbcTemplate.queryForObject(sql, new Object[]{userId, zonedDateTime.toLocalDate(), offset}, Long.class);

        } catch (DataAccessException e) {
            return 0L;
        }
    }

    Integer getOffset(Long userId, ZonedDateTime zonedDateTime) {
        String sql = "SELECT offset\n" +
                "                           FROM " + OLD_DB_SHEMA_NAME + ".activity_summaries\n" +
                "                           WHERE user_id = ? AND activity_day = ?  ORDER BY id ASC ";
        List<Integer> offsets = jdbcTemplate.query(sql, new Object[]{userId, zonedDateTime.toLocalDate()}, (rs, rowNum) -> rs.getInt(1));

        return offsets.size() >= 1 ? offsets.get(0) : 0;
    }

    List<Long> getNotMergedUserIds() {
        String sql = "SELECT id FROM users u WHERE id NOT IN (SELECT  new_user_id FROM " + OLD_DB_SHEMA_NAME + ".users_with_duplications_emails )";
        return jdbcTemplate.query(sql, (rs, rowNum) -> rs.getLong(1));
    }
}
