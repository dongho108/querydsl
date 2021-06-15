package study.querydsl;

import com.querydsl.core.BooleanBuilder;
import com.querydsl.core.QueryResults;
import com.querydsl.core.Tuple;
import com.querydsl.core.types.ExpressionUtils;
import com.querydsl.core.types.Projections;
import com.querydsl.core.types.dsl.CaseBuilder;
import com.querydsl.core.types.dsl.Expressions;
import com.querydsl.jpa.JPAExpressions;
import com.querydsl.jpa.impl.JPAQueryFactory;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.transaction.annotation.Transactional;
import study.querydsl.dto.MemberDto;
import study.querydsl.dto.QMemberDto;
import study.querydsl.dto.UserDto;
import study.querydsl.entity.Member;
import study.querydsl.entity.QMember;
import study.querydsl.entity.QTeam;
import study.querydsl.entity.Team;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.PersistenceUnit;

import java.util.List;

import static org.assertj.core.api.Assertions.*;
import static study.querydsl.entity.QMember.*;
import static study.querydsl.entity.QTeam.*;

@SpringBootTest
@Transactional
public class QuerydslBasicTest {

    @Autowired
    EntityManager em;

    JPAQueryFactory queryFactory;

    @BeforeEach
    public void before() {
        //given
        queryFactory = new JPAQueryFactory(em);
        Team teamA = new Team("teamA");
        Team teamB = new Team("teamB");
        em.persist(teamA);
        em.persist(teamB);

        //when
        Member member1 = new Member("member1", 10, teamA);
        Member member2 = new Member("member2", 20, teamA);
        Member member3 = new Member("member3", 30, teamB);
        Member member4 = new Member("member4", 40, teamB);
        em.persist(member1);
        em.persist(member2);
        em.persist(member3);
        em.persist(member4);
    }

    @Test
    public void startJPQL() throws Exception {
        //member1을 찾아라
        String qlString =
                "select m from Member m " +
                "where m.username = :username";

        Member findMember = em.createQuery(qlString, Member.class)
                .setParameter("username", "member1")
                .getSingleResult();

        assertThat(findMember.getUsername()).isEqualTo("member1");
    }

    @Test
    public void startQuerydsl() throws Exception {
        QMember m = new QMember("m");
//        QMember m = QMember.member;

        Member findMember = queryFactory
                .select(member)
                .from(member)
                .where(member.username.eq("member1"))
                .fetchOne();

        assertThat(findMember.getUsername()).isEqualTo("member1");
    }

    @Test
    public void search() throws Exception {
        Member findMember = queryFactory
                .selectFrom(member)
                .where(member.username.eq("member1")
                        .and(member.age.between(10, 30)))
                .fetchOne();

        assertThat(findMember.getUsername()).isEqualTo("member1");
    }

    @Test
    public void searchAndParam() throws Exception {
        Member findMember = queryFactory
                .selectFrom(member)
                .where(
                        member.username.eq("member1"),
                        member.age.eq(10)
                )
                .fetchOne();

        assertThat(findMember.getUsername()).isEqualTo("member1");
    }

    @Test
    public void resultFetch() throws Exception {

        // 리스트
        List<Member> fetch = queryFactory
                .selectFrom(member)
                .fetch();

        //단건
        Member fetchOne = queryFactory
                .selectFrom(member)
                .fetchOne();

        //처음 한 건
        Member fetchFirst = queryFactory
                .selectFrom(member)
                .fetchFirst();

        // 페이징에서 사용
        QueryResults<Member> results = queryFactory
                .selectFrom(member)
                .fetchResults();

        results.getTotal();
        List<Member> content = results.getResults();


        // count 쿼리만
        long total = queryFactory
                .selectFrom(member)
                .fetchCount();

    }

    /**
     * 회원 정렬 순서
     * 1. 회원 나이 내림차순 (desc)
     * 2. 회원 이름 올림차순 (asc)
     * 단 2에서 회원 이름이 없으면 마지막에 출력 (null last)
     */

    @Test
    public void sort() throws Exception {
        //given
        em.persist(new Member(null, 100));
        em.persist(new Member("member5", 100));
        em.persist(new Member("member6", 100));
        //when

        List<Member> result = queryFactory
                .selectFrom(member)
                .orderBy(member.age.desc(), member.username.asc().nullsLast())
                .fetch();

        //then

        Member member5 = result.get(0);
        Member member6 = result.get(1);
        Member memberNull = result.get(2);

        for (Member member1 : result) {
            System.out.println("member = " + member1);
        }
//        assertThat(member5.getUsername()).isEqualTo("member5");
//        assertThat(member6.getUsername()).isEqualTo("member6");
//        assertThat(memberNull.getUsername()).isNull();
    }

    @Test
    public void paging() throws Exception {
        //given
        List<Member> result = queryFactory
                .selectFrom(member)
                .orderBy(member.username.desc())
                .offset(1)
                .limit(2)
                .fetch();

        //when

        //then
        assertThat(result.size()).isEqualTo(2);
    }

    @Test
    public void paging1() throws Exception {
        //given
        QueryResults<Member> queryResults = queryFactory
                .selectFrom(member)
                .orderBy(member.username.desc())
                .offset(1)
                .limit(2)
                .fetchResults();

        //when

        //then
        assertThat(queryResults.getTotal()).isEqualTo(4);
        assertThat(queryResults.getLimit()).isEqualTo(2);
        assertThat(queryResults.getOffset()).isEqualTo(1);
        assertThat(queryResults.getResults().size()).isEqualTo(2);
    }

    @Test
    public void aggregation() throws Exception {
        //given
        List<Tuple> result = queryFactory
                .select(
                        member.count(),
                        member.age.sum(),
                        member.age.avg(),
                        member.age.max(),
                        member.age.min()
                )
                .from(member)
                .fetch();

        //when

        Tuple tuple = result.get(0);

        //then
        assertThat(tuple.get(member.count())).isEqualTo(4);
        assertThat(tuple.get(member.age.sum())).isEqualTo(100);
        assertThat(tuple.get(member.age.avg())).isEqualTo(25);
        assertThat(tuple.get(member.age.max())).isEqualTo(40);
        assertThat(tuple.get(member.age.min())).isEqualTo(10);
    }

    /**
     * 팀의 이름과 각 팀의 평균 연령을 구해라
     * @throws Exception
     */
    @Test
    public void group() throws Exception {
        //given
        List<Tuple> result = queryFactory
                .select(team.name, member.age.avg())
                .from(member)
                .join(member.team, team) // member에 있는 team과 team을 조인
                .groupBy(team.name)
                .fetch();

        //when

        Tuple teamA = result.get(0);
        Tuple teamB = result.get(1);


        //then
        assertThat(teamA.get(team.name)).isEqualTo("teamA");
        assertThat(teamA.get(member.age.avg())).isEqualTo(15); // (10+20) / 2

        assertThat(teamB.get(team.name)).isEqualTo("teamB");
        assertThat(teamB.get(member.age.avg())).isEqualTo(35); // (30+40) / 2
    }


    /**
     * 팀 A에 소속된 모든 회원
     * @throws Exception
     */
    @Test
    public void join() throws Exception {
        //given
        List<Member> result = queryFactory
                .selectFrom(member)
//                .join(member.team, team)
//                .leftJoin(member.team, team)
                .rightJoin(member.team, team)
                .where(team.name.eq("teamA"))
                .fetch();
        //when

        //then

        assertThat(result)
                .extracting("username")
                .containsExactly("member1", "member2");
    }


    /**
     * 세타 조인
     * 회원의 이름이 팀 이름과 같은 회원 조회
     * @throws Exception
     */
    @Test
    public void theta_join() throws Exception {
        //given
        em.persist(new Member("teamA"));
        em.persist(new Member("teamB"));
        em.persist(new Member("teamC"));
        //when

        List<Member> result = queryFactory
                .select(member)
                .from(member, team)
                .where(member.username.eq(team.name))
                .fetch();

        //then
        assertThat(result)
                .extracting("username")
                .containsExactly("teamA", "teamB");
    }

    /**
     * 예) 회원과 팀을 조인하면서, 팀 이름이 teamA인 팀만 조인, 회원은 모두 조회
     * JPQL : select m, t from Member m left join m.team on t.name = 'teamA'
     *
     * @throws Exception
     */
    @Test
    public void join_on_filtering() throws Exception {
        //given
        List<Tuple> result = queryFactory
                .select(member, team)
                .from(member)
                .leftJoin(member.team, team).on(team.name.eq("teamA"))
//                .join(member.team, team).on(team.name.eq("teamA"))
//                .join(member.team, team)
//                .where(team.name.eq("teamA"))
                .fetch();
        //when

        for (Tuple tuple : result) {
            System.out.println("tuple = " + tuple);
        }

        //then
    }


    /**
     * 연관관계가 없는 엔티티 외부 조인
     * 회원의 이름이 팀 이름과 같은 대상 외부 조인
     * @throws Exception
     */
    @Test
    public void join_on_no_relation() throws Exception {
        //given
        em.persist(new Member("teamA"));
        em.persist(new Member("teamB"));
        em.persist(new Member("teamC"));
        //when

        List<Tuple> result = queryFactory
                .select(member, team)
                .from(member)
//                .join(team).on(member.username.eq(team.name))
                .leftJoin(team).on(member.username.eq(team.name))
                .fetch();

        //then
        for (Tuple tuple : result) {
            System.out.println("tuple = " + tuple);
        }
    }


    @PersistenceUnit
    EntityManagerFactory emf;

    @Test
    public void fetchJoinNo() throws Exception {
        //given
        em.flush();
        em.clear();

        Member findMember = queryFactory
                .selectFrom(member)
                .where(member.username.eq("member1"))
                .fetchOne();

        //when
        boolean loaded = emf.getPersistenceUnitUtil().isLoaded(findMember.getTeam());
        assertThat(loaded).as("페치 조인 미적용").isFalse();


        //then
    }

    @Test
    public void fetchJoinUse() throws Exception {
        //given
        em.flush();
        em.clear();

        Member findMember = queryFactory
                .selectFrom(member)
                .join(member.team, team).fetchJoin()
                .where(member.username.eq("member1"))
                .fetchOne();

        //when
        boolean loaded = emf.getPersistenceUnitUtil().isLoaded(findMember.getTeam());
        assertThat(loaded).as("페치 조인 미적용").isTrue();


        //then
    }

    /**
     * 나이가 가장 많은 회원 조회
     * @throws Exception
     */
    @Test
    public void subQuery() throws Exception {

        QMember memberSub = new QMember("memberSub");
        //given
        List<Member> result = queryFactory
                .selectFrom(member)
                .where(member.age.eq(
                        JPAExpressions
                                .select(memberSub.age.max())
                                .from(memberSub)
                ))
                .fetch();

        //when
        for (Member member1 : result) {
            System.out.println("member1 = " + member1);
        }

        //then
        assertThat(result).extracting("age")
                .containsExactly(40);
    }

    /**
     * 나이가 평균이상인 회원 조회
     * @throws Exception
     */
    @Test
    public void subQueryGoe() throws Exception {

        QMember memberSub = new QMember("memberSub");
        //given
        List<Member> result = queryFactory
                .selectFrom(member)
                .where(member.age.goe(
                        JPAExpressions
                                .select(memberSub.age.avg())
                                .from(memberSub)
                ))
                .fetch();

        //when
        for (Member member1 : result) {
            System.out.println("member1 = " + member1);
        }

        //then
        assertThat(result).extracting("age")
                .containsExactly(30, 40);
    }

    /**
     * 나이가 평균이상인 회원 조회
     * @throws Exception
     */
    @Test
    public void subQueryIn() throws Exception {

        QMember memberSub = new QMember("memberSub");
        //given
        List<Member> result = queryFactory
                .selectFrom(member)
                .where(member.age.in(
                        JPAExpressions
                                .select(memberSub.age)
                                .from(memberSub)
                                .where(memberSub.age.gt(10))
                ))
                .fetch();

        //when
        for (Member member1 : result) {
            System.out.println("member1 = " + member1);
        }

        //then
        assertThat(result).extracting("age")
                .containsExactly(20, 30, 40);
    }

    @Test
    public void selectSubQuery() throws Exception {

        QMember memberSub = new QMember("memberSub");


        //given
        List<Tuple> result = queryFactory
                .select(member.age,
                        JPAExpressions
                                .select(memberSub.age.avg())
                                .from(memberSub))
                .from(member)
                .fetch();

        //when

        for (Tuple tuple : result) {
            System.out.println("tuple = " + tuple);
        }

        //then
    }

    @Test
    public void basicCase() throws Exception {
        //given
        List<String> fetch = queryFactory
                .select(member.age
                        .when(10).then("열살")
                        .when(20).then("스무살")
                        .otherwise("기타"))
                .from(member)
                .fetch();

        //when
        for (String s : fetch) {
            System.out.println("s = " + s);
        }

        //then
    }

    @Test
    public void complexCase() throws Exception {
        //given
        List<String> result = queryFactory
                .select(new CaseBuilder()
                        .when(member.age.between(0, 20)).then("0~20살")
                        .when(member.age.between(21, 30)).then("21~30살")
                        .otherwise("기타"))
                .from(member)
                .fetch();

        //when
        for (String s : result) {
            System.out.println("s = " + s);
        }

        //then
    }

    @Test
    public void constant() throws Exception {
        //given
        List<Tuple> result = queryFactory
                .select(member.username, Expressions.constant("A"))
                .from(member)
                .fetch();

        //when
        for (Tuple tuple : result) {
            System.out.println("tuple = " + tuple);
        }

        //then
    }

    @Test
    public void concat() throws Exception {
        //given

        //{username}_{age}
        List<String> result = queryFactory
                .select(member.username.concat("_").concat(member.age.stringValue()))
                .from(member)
                .where(member.username.eq("member1"))
                .fetch();

        //when

        for (String s : result) {
            System.out.println("s = " + s);
        }

        //then
    }

    @Test
    public void simpleProjection() throws Exception {
        //given
        List<String> result = queryFactory
                .select(member.username)
                .from(member)
                .fetch();
        //when

        for (String s : result) {
            System.out.println("s = " + s);
        }

        //then
    }

    @Test
    public void tupleProjection() throws Exception {
        //given
        List<Tuple> result = queryFactory
                .select(member.username, member.age)
                .from(member)
                .fetch();

        //when
        for (Tuple tuple : result) {
            String username = tuple.get(member.username);
            Integer age = tuple.get(member.age);
            System.out.println("username = " + username);
            System.out.println("age = " + age);
        }

        //then
    }

    @Test
    public void findDtoByJPQL() throws Exception {
        //given
        List<MemberDto> result = em.createQuery("select new study.querydsl.dto.MemberDto(m.username, m.age) from Member m", MemberDto.class)
                .getResultList();
        //when

        for (MemberDto memberDto : result) {
            System.out.println("memberDto = " + memberDto);
        }

        //then
    }

    @Test
    public void findDtoBySetter() throws Exception {
        //given
        List<MemberDto> result = queryFactory
                .select(Projections.bean(MemberDto.class,
                        member.username,
                        member.age))
                .from(member)
                .fetch();

        //when
        for (MemberDto memberDto : result) {
            System.out.println("memberDto = " + memberDto);
        }

        //then
    }

    @Test
    public void findDtoByField() throws Exception {
        //given
        List<MemberDto> result = queryFactory
                .select(Projections.fields(MemberDto.class,
                        member.username,
                        member.age))
                .from(member)
                .fetch();

        //when
        for (MemberDto memberDto : result) {
            System.out.println("memberDto = " + memberDto);
        }

        //then
    }

    @Test
    public void findUserDto() throws Exception {
        QMember memberSub = new QMember("memberSub");
        //given
        List<UserDto> result = queryFactory
                .select(Projections.fields(UserDto.class,
                        member.username.as("name"),
//                        ExpressionUtils.as(member.username, "name"),

                        ExpressionUtils.as(JPAExpressions
                                .select(memberSub.age.max())
                                    .from(memberSub), "age")
                ))
                .from(member)
                .fetch();


        //when
        for (UserDto userDto : result) {
            System.out.println("userDto = " + userDto);
        }

        //then
    }

    @Test
    public void findDtoByConstructor() throws Exception {
        //given
        List<MemberDto> result = queryFactory
                .select(Projections.constructor(MemberDto.class,
                        member.username,
                        member.age))
                .from(member)
                .fetch();

//        List<UserDto> result = queryFactory
//                .select(Projections.constructor(UserDto.class,
//                        member.username,
//                        member.age))
//                .from(member)
//                .fetch();

        //when
        for (MemberDto memberDto : result) {
            System.out.println("memberDto = " + memberDto);
        }

//        for (UserDto userDto : result) {
//            System.out.println("userDto = " + userDto);
//        }

        //then
    }

    @Test
    public void findDtoByQueryProjection() throws Exception {
        //given
        List<MemberDto> result = queryFactory
                .select(new QMemberDto(member.username, member.age))
                .from(member)
                .fetch();
        //when

        for (MemberDto memberDto : result) {
            System.out.println("memberDto = " + memberDto);
        }

        //then
    }

    @Test
    public void dynamicQuery_BooleanBuilder() throws Exception {
        //given
        String usernameParam = "member1";
        Integer ageParam = null;

        List<Member> result = searchMember1(usernameParam, ageParam);
        assertThat(result.size()).isEqualTo(1);

        //when

        //then
    }

    private List<Member> searchMember1(String usernameCond, Integer ageCond) {

        BooleanBuilder builder = new BooleanBuilder(member.username.eq(usernameCond));
        if (usernameCond != null) {
            builder.and(member.username.eq(usernameCond));
        }

        if (ageCond != null) {
            builder.and(member.age.eq(ageCond));
        }


        return queryFactory
                .selectFrom(member)
                .where(builder)
                .fetch();

    }
}
