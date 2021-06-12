

        package Implementation;

        import cn.edu.sustech.cs307.database.SQLDataSource;
        import cn.edu.sustech.cs307.dto.*;
        import cn.edu.sustech.cs307.dto.grade.Grade;
        import cn.edu.sustech.cs307.dto.grade.HundredMarkGrade;
        import cn.edu.sustech.cs307.dto.grade.PassOrFailGrade;
        import cn.edu.sustech.cs307.dto.prerequisite.AndPrerequisite;
        import cn.edu.sustech.cs307.dto.prerequisite.CoursePrerequisite;
        import cn.edu.sustech.cs307.dto.prerequisite.OrPrerequisite;
        import cn.edu.sustech.cs307.dto.prerequisite.Prerequisite;
        import cn.edu.sustech.cs307.exception.*;
        import cn.edu.sustech.cs307.service.StudentService;
        import org.postgresql.util.PSQLException;

        import javax.annotation.Nullable;
        import java.sql.*;
        import java.sql.Date;
        import java.text.SimpleDateFormat;
        import java.time.DayOfWeek;
        import java.util.*;

public class ReferenceStudentService implements StudentService {
    @Override
    public synchronized void addStudent(int userId, int majorId, String firstName, String lastName, Date enrolledDate) {
        try{
            if (userId < 0)
            {
                throw new IntegrityViolationException();
            }
            if (majorId < 0)
            {
                throw new IntegrityViolationException();
            }
            Connection connection = SQLDataSource.getInstance().getSQLConnection();
            PreparedStatement pst;
            String sql = "INSERT INTO user1(id,firstname,lastname,enrolleddate,majorid,type) values(?,?,?,?,?,?)";
            pst = (PreparedStatement) connection.prepareStatement(sql);
            pst.setInt(1, userId);
            pst.setString(2, firstName);
            pst.setString(3, lastName);
            pst.setDate(4, enrolledDate);
            pst.setInt(5, majorId);
            pst.setInt(6,1);
            pst.executeUpdate();
            connection.close();
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    @Override
    public List<CourseSearchEntry> searchCourse(int studentId, int semesterId, @Nullable String searchCid, @Nullable String searchName, @Nullable String searchInstructor, @Nullable DayOfWeek searchDayOfWeek, @Nullable Short searchClassTime, @Nullable List<String> searchClassLocations, CourseType searchCourseType, boolean ignoreFull, boolean ignoreConflict, boolean ignorePassed, boolean ignoreMissingPrerequisites, int pageSize, int pageIndex) {
        try {
            Connection connection = SQLDataSource.getInstance().getSQLConnection();
            connection.setAutoCommit(false);
            connection.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
            PreparedStatement pst;
            ResultSet rst;
            String sql;
            Major major = getStudentMajor(studentId);
            sql = "select course.id, course.name, course.credit, course.classhour, course.grading, "
                    + "coursesection.id, coursesection.sectionname, coursesection.totalcapacity, coursesection.leftcapacity, "
                    + "coursesectionclass.dayofweek, coursesectionclass.weeklist, coursesectionclass.classbegin, "
                    + "coursesectionclass.classend, coursesectionclass.location "
                    + "from "
                    + "course join coursesection on coursesection.courseid = course.id "
                    + "left outer join coursesectionclass on coursesectionclass.coursesectionid = coursesection.id "
                    + "join user1 on user1.id = coursesectionclass.instructor and user1.type = 2 ";
            switch (searchCourseType)
            {
                case ALL:
                    break;
                case MAJOR_COMPULSORY:
                    sql = sql + "left outer join major_course on major_course.courseid = coursesection.courseid ";
                    break;
                case MAJOR_ELECTIVE:
                    sql = sql + "left outer join major_course on major_course.courseid = coursesection.courseid ";
                    break;
                case CROSS_MAJOR:
                    sql = sql + "left outer join major_course on major_course.courseid = coursesection.courseid ";
                    break;
                case PUBLIC:
                    sql = sql + "left outer join major_course on major_course.courseid = coursesection.courseid ";
                    break;
            }
            sql = sql + " where coursesection.semesterid = ? ";
            switch (searchCourseType)
            {
                case ALL:
                    break;
                case MAJOR_COMPULSORY:
                    sql = sql + "and major_course.majorid is not null "
                            + "and major_course.type = 1 "
                            + "and major_course.majorid = ? ";
                    break;
                case MAJOR_ELECTIVE:
                    sql = sql + "and major_course.majorid is not null "
                            + "and major_course.type = 2 "
                            + "and major_course.majorid = ? ";
                    break;
                case CROSS_MAJOR:
                    sql = sql + "and major_course.majorid is not null "
                            + "and major_course.majorid <> ? ";
                    break;
                case PUBLIC:
                    sql = sql + "and major_course.courseid is null ";
                    break;
            }
            if (searchCid != null)
            {
                sql = sql + "and position(? in coursesection.courseid) > 0 ";
            }
            if (searchName != null)
            {
                sql = sql + "and position(? in (course.name||'['||coursesection.sectionname||']')) > 0 ";
            }
            if (searchInstructor != null)
            {
                String subq = "user1.firstname||user1.lastname like (?)||'%' "
                        + "or user1.firstname||' '||user1.lastname like (?)||'%' "
                        + "or user1.firstname like (?)||'%' "
                        + "or user1.lastname like (?)||'%' ";
                sql = sql + "and (" + subq + ") ";
            }
            if (searchDayOfWeek != null)
            {
                sql = sql + "and coursesectionclass.dayofweek = ? ";
            }
            if (searchClassTime != null)
            {
                sql = sql + "and coursesectionclass.classbegin <= ? and coursesectionclass.classend >= ? ";
            }
            if (searchClassLocations != null)
            {
                int len = searchClassLocations.size();
                if (len > 0)
                {
                    sql = sql + "and (";
                    for (int i = 0; i < len - 1; i++)
                    {
                        sql = sql + " position(? in coursesectionclass.location) > 0 or ";
                    }
                    sql = sql + "position(? in coursesectionclass.location) > 0 ) ";
                }
                else
                {
                    sql = sql + "and false ";
                }
            }
            if (ignoreFull)
            {
                sql = sql + "and coursesection.leftcapacity > 0 ";
            }
            pst = (PreparedStatement) connection.prepareStatement(sql);
            int pos = 0;
            if (searchCourseType == CourseType.MAJOR_COMPULSORY || searchCourseType == CourseType.MAJOR_ELECTIVE || searchCourseType == CourseType.CROSS_MAJOR)
            {
                pst.setInt(2, major.id);
                pst.setInt(1, semesterId);
                pos = 2;
            }
            else {
                pst.setInt(1, semesterId);
                pos = 1;
            }
            if (searchCid != null)
            {
                pos++;
                pst.setString(pos, searchCid);
            }
            if (searchName != null)
            {
                pos++;
                pst.setString(pos, searchName);
            }
            if (searchInstructor != null)
            {
                pos++;
                pst.setString(pos, searchInstructor);
                pos++;
                pst.setString(pos, searchInstructor);
                pos++;
                pst.setString(pos, searchInstructor);
                pos++;
                pst.setString(pos, searchInstructor);
            }
            if (searchDayOfWeek != null)
            {
                pos++;
                switch (searchDayOfWeek)
                {
                    case MONDAY:
                        pst.setInt(pos, 1);
                        break;
                    case TUESDAY:
                        pst.setInt(pos, 2);
                        break;
                    case WEDNESDAY:
                        pst.setInt(pos, 3);
                        break;
                    case THURSDAY:
                        pst.setInt(pos, 4);
                        break;
                    case FRIDAY:
                        pst.setInt(pos, 5);
                        break;
                    case SATURDAY:
                        pst.setInt(pos, 6);
                        break;
                    case SUNDAY:
                        pst.setInt(pos, 7);
                        break;
                    default:
                }
            }
            if (searchClassTime != null)
            {
                pos++;
                pst.setShort(pos, searchClassTime);
                pos++;
                pst.setShort(pos, searchClassTime);
            }
            if (searchClassLocations != null)
            {
                for (String searchClassLocation : searchClassLocations) {
                    pos++;
                    pst.setString(pos, searchClassLocation);
                }
            }
            rst = pst.executeQuery();
            List<CourseSearchEntry> result = new ArrayList<CourseSearchEntry>();
            Map<Integer, List<String>> havesection = new HashMap<>();
            Map<Integer, Integer> conflictids = new HashMap<>();
            while(rst.next())
            {
                String courseid = rst.getString(1);
                String coursename = rst.getString(2);
                int credit = rst.getInt(3);
                int classhour = rst.getInt(4);
                String grading = rst.getString(5);
                int sectionid = rst.getInt(6);
                String sectionname = rst.getString(7);
                int totalcapacity = rst.getInt(8);
                int leftcapacity = rst.getInt(9);
                if (ignoreMissingPrerequisites)
                {
                    if (!passedPrerequisitesForCourse(studentId, courseid))
                    {
                        continue;
                    }
                }
                if (ignorePassed)
                {
                    String subsql = "select * from studentgrades where studentid = ? "
                            + "and courseid = ? and pf = 1";
                    PreparedStatement subpst = (PreparedStatement) connection.prepareStatement(subsql);
                    subpst.setInt(1, studentId);
                    subpst.setString(2, courseid);
                    ResultSet subrst = subpst.executeQuery();
                    if (subrst.next())
                    {
                        continue;
                    }
                }
                List<String> conflictCourseNames = new ArrayList<String>();
                Object o;
                o = rst.getObject(10);
                int dayofweek = -1;
                Array array;
                List<Short> weeklist = new ArrayList<Short>();
                short classbegin = -1;
                short classend = -1;
                String location = null;
                if (o != null)
                {
                    dayofweek = rst.getInt(10);
                    array = rst.getArray(11);
                    if (array != null)
                    {
                        ResultSet arr = array.getResultSet();
                        while (arr.next())
                        {
                            weeklist.add(arr.getShort(2));
                        }
                    }
                    classbegin = rst.getShort(12);
                    classend = rst.getShort(13);
                    location = rst.getString(14);
                }
                String subsql = "select coursesectionclass.dayofweek, coursesectionclass.weeklist, "
                        + "coursesectionclass.classbegin, coursesectionclass.classend, coursesectionclass.location, "
                        + "course.name, coursesection.sectionname, course.id, "
                        + "studentgrades.sectionid "
                        + "from studentgrades join coursesection on studentgrades.sectionid = coursesection.id "
                        + "join course on studentgrades.courseid = course.id "
                        + "left outer join coursesectionclass on coursesection.id = coursesectionclass.coursesectionid "
                        + "where studentgrades.studentid = ? and studentgrades.semesterid = ?";
                //+ "and studentgrades.grade is null and studentgrades.pf is null";
                PreparedStatement subpst = (PreparedStatement) connection.prepareStatement(subsql);
                subpst.setInt(1, studentId);
                subpst.setInt(2, semesterId);
                ResultSet subrst = subpst.executeQuery();
                boolean isconflict = false;
                while (subrst.next())
                {
                    if (subrst.getString(8).equals(courseid))
                    {
                        if (!ignoreConflict)
                        {
                            String coursefullname = String.format("%s[%s]", subrst.getString(6), subrst.getString(7));
                            if (!(conflictCourseNames.contains(coursefullname)))
                            {
                                conflictCourseNames.add(coursefullname);
                            }
                        }
                        else
                        {
                            isconflict = true;
                        }
                    }
                    else
                    {
                        Object subo = subrst.getObject(1);
                        int tmpdayofweek = -2;
                        List<Short> tmpweeklist = new ArrayList<Short>();
                        short tmpclassbegin = -1, tmpclassend = -1;
                        String tmplocation = null;
                        if (subo != null)
                        {
                            tmpdayofweek = subrst.getInt(1);
                            Array tmparray = subrst.getArray(2);
                            if (tmparray != null)
                            {
                                ResultSet arr = tmparray.getResultSet();
                                while (arr.next())
                                {
                                    tmpweeklist.add(arr.getShort(2));
                                }
                            }
                            tmpclassbegin = subrst.getShort(3);
                            tmpclassend = subrst.getShort(4);
                            tmplocation = subrst.getString(5);
                        }
                        for (short i : weeklist)
                        {
                            for (short j : tmpweeklist)
                            {
                                if (i == j && dayofweek == tmpdayofweek &&
                                        ((classbegin <= tmpclassbegin && tmpclassbegin <= classend) ||
                                                (classbegin <= tmpclassend && tmpclassend <= classend)) )
                                {
                                    if (!ignoreConflict)
                                    {
                                        String coursefullname = String.format("%s[%s]", subrst.getString(6), subrst.getString(7));
                                        if (!(conflictCourseNames.contains(coursefullname)))
                                        {
                                            conflictCourseNames.add(coursefullname);
                                        }
                                    }
                                    else
                                    {
                                        isconflict = true;
                                        break;
                                    }
                                }
                            }
                            if (ignoreConflict && isconflict)
                            {
                                break;
                            }
                        }
                    }
                    if (ignoreConflict && isconflict)
                    {
                        break;
                    }
                }
                if (!ignoreConflict)
                {
                    conflictCourseNames.sort(new Comparator<String>() {

                        @Override
                        public int compare(String o1, String o2) {
                            if (o1.equalsIgnoreCase(o2)) {
                                return 0;
                            } else {
                                return o1.toUpperCase().compareTo(o2.toUpperCase()) > 0 ? 1 : -1;
                            }
                        }

                    });
                }
                if (isconflict && ignoreConflict)
                {
                    conflictids.put(sectionid, 1);
                    havesection.remove(sectionid);
                }
                else
                {
                    if (havesection.containsKey(sectionid))
                    {
                        havesection.get(sectionid).addAll(conflictCourseNames);
                    }
                    else
                    {
                        if (!conflictids.containsKey(sectionid))
                        {
                            havesection.put(sectionid, conflictCourseNames);
                        }
                    }
                }
            }
            Set<Map.Entry<Integer, List<String>>> entrySet = havesection.entrySet();
            for (Map.Entry<Integer, List<String>> entry : entrySet)
            {
                int sectionid = entry.getKey();
                String subsql = "select coursesectionclass.id, coursesectionclass.dayofweek, coursesectionclass.weeklist, "
                        + "coursesectionclass.classbegin, coursesectionclass.classend, coursesectionclass.location, "
                        + "user1.firstname, user1.lastname, user1.id "
                        + "from coursesectionclass join user1 on coursesectionclass.instructor = user1.id and type = 2"
                        + "where coursesectionclass.coursesectionid = ?";
                PreparedStatement subpst = (PreparedStatement) connection.prepareStatement(subsql);
                subpst.setInt(1, sectionid);
                ResultSet subrst = subpst.executeQuery();
                Set<CourseSectionClass> sectionClasses = new HashSet<CourseSectionClass>();
                while (subrst.next())
                {
                    int coursesectionclassid = subrst.getInt(1);
                    DayOfWeek dayweek;
                    switch (subrst.getInt(2))
                    {
                        case 1:
                            dayweek = DayOfWeek.MONDAY;
                            break;
                        case 2:
                            dayweek = DayOfWeek.TUESDAY;
                            break;
                        case 3:
                            dayweek = DayOfWeek.WEDNESDAY;
                            break;
                        case 4:
                            dayweek = DayOfWeek.THURSDAY;
                            break;
                        case 5:
                            dayweek = DayOfWeek.FRIDAY;
                            break;
                        case 6:
                            dayweek = DayOfWeek.SATURDAY;
                            break;
                        case 7:
                            dayweek = DayOfWeek.SUNDAY;
                            break;
                        default:
                            dayweek = null;
                    }
                    Array array = subrst.getArray(3);
                    List<Short> weeklist = new ArrayList<Short>();
                    if (array != null)
                    {
                        ResultSet arr = array.getResultSet();
                        while (arr.next())
                        {
                            weeklist.add(arr.getShort(2));
                        }
                    }
                    short classbegin = subrst.getShort(4);
                    short classend = subrst.getShort(5);
                    String location = subrst.getString(6);
                    String firstname = subrst.getString(7);
                    String lastname = subrst.getString(8);
                    String fullname;
                    if(firstname.matches("^[A-Z\\sa-z]+$") && lastname.matches("^[A-Z\\sa-z]+$"))
                        fullname = firstname + " " + lastname;
                    else
                        fullname = firstname + lastname;
                    int instructorid = subrst.getInt(9);
                    Instructor instructor = new Instructor();
                    instructor.id = instructorid;
                    instructor.fullName = fullname;
                    CourseSectionClass coursesectionclass = new CourseSectionClass();
                    coursesectionclass.id = coursesectionclassid;
                    coursesectionclass.instructor = instructor;
                    coursesectionclass.dayOfWeek = dayweek;
                    coursesectionclass.weekList = new HashSet<>(weeklist);
                    coursesectionclass.classBegin = classbegin;
                    coursesectionclass.classEnd = classend;
                    coursesectionclass.location = location;
                    sectionClasses.add(coursesectionclass);
                }
                subsql = "select course.id, course.name, course.credit, course.classhour, course.grading, "
                        + "coursesection.id, coursesection.sectionname, coursesection.totalcapacity, coursesection.leftcapacity "
                        + "from course join coursesection on course.id = coursesection.courseid "
                        + "where coursesection.id = ?";
                subpst = (PreparedStatement) connection.prepareStatement(subsql);
                subpst.setInt(1, sectionid);
                subrst = subpst.executeQuery();
                if (subrst.next())
                {
                    CourseSearchEntry courseSearchEntry = new CourseSearchEntry();
                    Course course = new Course();
                    course.id = subrst.getString(1);
                    course.name = subrst.getString(2);
                    course.credit = subrst.getInt(3);
                    course.classHour = subrst.getInt(4);
                    course.grading = (subrst.getString(5).equals("HundredMarkScore")? Course.CourseGrading.HUNDRED_MARK_SCORE:Course.CourseGrading.PASS_OR_FAIL);
                    CourseSection coursesection = new CourseSection();
                    coursesection.id = sectionid;
                    coursesection.name = subrst.getString(7);
                    coursesection.totalCapacity = subrst.getInt(8);
                    coursesection.leftCapacity = subrst.getInt(9);
                    courseSearchEntry.course = course;
                    courseSearchEntry.section = coursesection;
                    courseSearchEntry.sectionClasses = sectionClasses;
                    courseSearchEntry.conflictCourseNames = entry.getValue();
                    result.add(courseSearchEntry);
                }
            }
            result.sort(new Comparator<CourseSearchEntry>() {

                @Override
                public int compare(CourseSearchEntry o1, CourseSearchEntry o2) {
                    String id1 = o1.course.id;
                    String id2 = o2.course.id;
                    if (id1.equalsIgnoreCase(id2)) {
                        String name1 = String.format("%s[%s]", o1.course.name, o1.section.name);
                        String name2 = String.format("%s[%s]", o2.course.name, o2.section.name);
                        if (name1.equalsIgnoreCase(name2)) {
                            return 0;
                        } else {
                            return name1.toUpperCase().compareTo(name2.toUpperCase()) > 0 ? 1 : -1;
                        }
                    } else {
                        return id1.toUpperCase().compareTo(id2.toUpperCase()) > 0 ? 1 : -1;
                    }
                }

            });
            List<CourseSearchEntry> ans = new ArrayList<CourseSearchEntry>();
            int start = pageIndex * pageSize;
            int len = result.size();
            int limit = Math.min(len, start + pageSize);
            for (int i = start; i < limit; i++)
            {
                ans.add(result.get(i));
            }
            connection.commit();
            connection.close();
            return ans;
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public EnrollResult enrollCourse(int studentId, int sectionId) {
        try{
            Connection connection = SQLDataSource.getInstance().getSQLConnection();
            connection.setAutoCommit(false);
            connection.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
            PreparedStatement pst = null;
            String sql = "select courseid, semesterid from coursesection where id = ?";
            pst = (PreparedStatement) connection.prepareStatement(sql);
            pst.setInt(1, sectionId);
            ResultSet rst = pst.executeQuery();
            String courseid;
            int semesterid;
            if (!rst.next())
            {
                connection.commit();
                connection.close();
                return EnrollResult.COURSE_NOT_FOUND;
            }
            else
            {
                courseid = rst.getString(1);
                semesterid = rst.getInt(2);
            }
            sql = "select grade, PF, semesterid, sectionid from studentgrades where studentid = ? and courseid = ?";
            pst = (PreparedStatement) connection.prepareStatement(sql);
            pst.setInt(1, studentId);
            pst.setString(2, courseid);
            rst = pst.executeQuery();
            while (rst.next())
            {
                Object grade = rst.getObject(1);
                Object pf = rst.getObject(2);
                int tmpsemester = rst.getInt(3);
                int tmpsectionid = rst.getInt(4);
                if (tmpsemester == semesterid && tmpsectionid == sectionId)
                {
                    connection.commit();
                    connection.close();
                    return EnrollResult.ALREADY_ENROLLED;
                }
                else if (pf != null && (int)pf == 1)
                {
                    connection.commit();
                    connection.close();
                    return EnrollResult.ALREADY_PASSED;
                }
                else if (tmpsemester == semesterid)
                {
                    if (!passedPrerequisitesForCourse(studentId, courseid))
                    {
                        connection.commit();
                        connection.close();
                        return EnrollResult.PREREQUISITES_NOT_FULFILLED;
                    }
                    else
                    {
                        connection.commit();
                        connection.close();
                        return EnrollResult.COURSE_CONFLICT_FOUND;
                    }
                }
//                else
//                {
//                    connection.commit();
//                    connection.close();
//                    return EnrollResult.UNKNOWN_ERROR;
//                }
            }
            sql = "select coursesection.courseid, coursesection.leftcapacity, "
                    + "coursesectionclass.dayofweek, coursesectionclass.weeklist, "
                    + "coursesectionclass.classbegin, coursesectionclass.classend, "
                    + "coursesectionclass.location "
                    + "from coursesection "
                    + "left outer join coursesectionclass on coursesection.id = coursesectionclass.coursesectionid "
                    + "where coursesection.id = ?";
            pst = (PreparedStatement) connection.prepareStatement(sql);
            pst.setInt(1, sectionId);
            rst = pst.executeQuery();
            int leftcapacity = 0, dayofweek = -1;
            short classbegin = -1, classend = -1;
            String location;
            boolean hasclass = false;
            while (rst.next())
            {
                List<Short> weeklist = new ArrayList<Short>();
                hasclass = true;
                courseid = rst.getString(1);
                if (!passedPrerequisitesForCourse(studentId, courseid))
                {
                    connection.commit();
                    connection.close();
                    return EnrollResult.PREREQUISITES_NOT_FULFILLED;
                }
                leftcapacity = rst.getInt(2);
                Object o = rst.getObject(3);
                if (o != null)
                {
                    dayofweek = rst.getInt(3);
                    Array array = rst.getArray(4);
                    if (array != null)
                    {
                        ResultSet arr = array.getResultSet();
                        while (arr.next())
                        {
                            weeklist.add(arr.getShort(2));
                        }
                    }
                    classbegin = rst.getShort(5);
                    classend = rst.getShort(6);
                    location = rst.getString(7);
                }
                String subsql = "select coursesection.courseid, coursesection.leftcapacity, "
                        + "coursesectionclass.dayofweek, coursesectionclass.weeklist, "
                        + "coursesectionclass.classbegin, coursesectionclass.classend, "
                        + "coursesectionclass.location "
                        + "from coursesection "
                        + "left outer join coursesectionclass on coursesection.id = coursesectionclass.coursesectionid "
                        + "join studentgrades on studentgrades.sectionid = coursesection.id "
                        + "where studentgrades.studentid = ? "
                        + "and studentgrades.semesterid = ?";
                PreparedStatement subpst = (PreparedStatement) connection.prepareStatement(subsql);
                subpst.setInt(1, studentId);
                subpst.setInt(2, semesterid);
                ResultSet subrst = subpst.executeQuery();
                while(subrst.next())
                {
                    String tmpcourseid = subrst.getString(1);
                    int tmpleftcapacity = subrst.getInt(2);
                    Object subo = subrst.getObject(3);
                    int tmpdayofweek = -2;
                    List<Short> tmpweeklist = new ArrayList<Short>();
                    short tmpclassbegin = -2, tmpclassend = -2;
                    String tmplocation = null;
                    if (subo != null)
                    {
                        tmpdayofweek = subrst.getInt(3);
                        Array tmparray = subrst.getArray(4);
                        if (tmparray != null)
                        {
                            ResultSet arr = tmparray.getResultSet();
                            while (arr.next())
                            {
                                tmpweeklist.add(arr.getShort(2));
                            }
                        }
                        tmpclassbegin = subrst.getShort(5);
                        tmpclassend = subrst.getShort(6);
                        tmplocation = subrst.getString(7);
                    }
                    if (tmpcourseid.equalsIgnoreCase(courseid))
                    {
                        connection.commit();
                        connection.close();
                        return EnrollResult.COURSE_CONFLICT_FOUND;
                    }
                    for (short i : weeklist)
                    {
                        for (short j : tmpweeklist)
                        {
                            if (i == j && dayofweek == tmpdayofweek &&
                                    ((classbegin <= tmpclassbegin && tmpclassbegin <= classend) ||
                                            (classbegin <= tmpclassend && tmpclassend <= classend)))
                            {
                                connection.commit();
                                connection.close();
                                return EnrollResult.COURSE_CONFLICT_FOUND;
                            }
                        }
                    }
                }
            }
            if (!hasclass)
            {
                sql = "select coursesection.courseid "
                        + "from coursesection "
                        + "where coursesection.id = ?";
                pst = (PreparedStatement) connection.prepareStatement(sql);
                pst.setInt(1, sectionId);
                rst = pst.executeQuery();
                if (rst.next())
                {
                    courseid = rst.getString(1);
                    sql = "select courseid from studentgrades "
                            + "where studentgrades.studentid = ? "
                            + "and studentgrades.semesterid = ?";
                    pst = (PreparedStatement) connection.prepareStatement(sql);
                    pst.setInt(1, studentId);
                    pst.setInt(2, semesterid);
                    rst = pst.executeQuery();
                    while (rst.next())
                    {
                        String tmpid = rst.getString(1);
                        if (tmpid.equalsIgnoreCase(courseid))
                        {
                            connection.commit();
                            connection.close();
                            return EnrollResult.COURSE_CONFLICT_FOUND;
                        }
                    }
                    if (passedPrerequisitesForCourse(studentId, courseid))
                    {
                        sql = "select leftcapacity from coursesection where id = ?";
                        pst = (PreparedStatement) connection.prepareStatement(sql);
                        pst.setInt(1, sectionId);
                        rst = pst.executeQuery();
                        if (rst.next())
                        {
                            leftcapacity = rst.getInt(1);
                        }
                        if (leftcapacity == 0)
                        {
                            connection.commit();
                            connection.close();
                            return EnrollResult.COURSE_IS_FULL;
                        }
                        sql = "update coursesection set leftcapacity = ? where id = ?";
                        pst = (PreparedStatement) connection.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
                        pst.setInt(1, leftcapacity - 1);
                        pst.setInt(2, sectionId);
                        pst.executeUpdate();
                        sql = "insert into studentgrades(studentid, sectionid, courseid, semesterid, grade, pf) "
                                + "values (?, ?, ?, ?, null, null)";
                        pst = (PreparedStatement) connection.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
                        pst.setInt(1, studentId);
                        pst.setInt(2, sectionId);
                        pst.setString(3, courseid);
                        pst.setInt(4, semesterid);
                        pst.executeUpdate();
                        connection.commit();
                        connection.close();
                        return EnrollResult.SUCCESS;
                    }
                    else
                    {
                        connection.commit();
                        connection.close();
                        return EnrollResult.PREREQUISITES_NOT_FULFILLED;
                    }
                }
                else
                {
                    return EnrollResult.UNKNOWN_ERROR;
                }
            }
            if (leftcapacity == 0)
            {
                connection.commit();
                connection.close();
                return EnrollResult.COURSE_IS_FULL;
            }
            sql = "update coursesection set leftcapacity = ? where id = ?";
            pst = (PreparedStatement) connection.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
            pst.setInt(1, leftcapacity - 1);
            pst.setInt(2, sectionId);
            pst.executeUpdate();
            sql = "insert into studentgrades(studentid, sectionid, courseid, semesterid, grade, pf) "
                    + "values (?, ?, ?, ?, null, null)";
            pst = (PreparedStatement) connection.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
            pst.setInt(1, studentId);
            pst.setInt(2, sectionId);
            pst.setString(3, courseid);
            pst.setInt(4, semesterid);
            pst.executeUpdate();
            connection.commit();
            connection.close();
            return EnrollResult.SUCCESS;
        }catch (Exception e){
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public void dropCourse(int studentId, int sectionId) throws IllegalStateException {
        try
        {
            Connection connection = SQLDataSource.getInstance().getSQLConnection();
            connection.setAutoCommit(false);
            connection.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
            PreparedStatement pst = null;
            String sql = "select grade, pf from studentgrades where studentid = ? and sectionid = ?";
            pst = (PreparedStatement) connection.prepareStatement(sql);
            pst.setInt(1, studentId);
            pst.setInt(2, sectionId);
            ResultSet rst = pst.executeQuery();
            if (rst.next())
            {
                Object grade = rst.getObject(1);
                Object pf = rst.getObject(2);
                if (pf != null)
                {
                    //connection.commit();
                    connection.close();
                    throw new IllegalStateException();
                }
                sql = "delete from studentgrades where studentid = ? and sectionid = ?";
                pst = (PreparedStatement) connection.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
                pst.setInt(1, studentId);
                pst.setInt(2, sectionId);
                pst.executeUpdate();
                sql = "select leftcapacity from coursesection where id = ?";
                pst = (PreparedStatement) connection.prepareStatement(sql);
                pst.setInt(1, sectionId);
                rst = pst.executeQuery();
                if (rst.next())
                {
                    int leftcapacity = rst.getInt(1);
                    sql = "update coursesection set leftcapacity = ? where id = ?";
                    pst = (PreparedStatement) connection.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
                    pst.setInt(1, leftcapacity + 1);
                    pst.setInt(2, sectionId);
                    pst.executeUpdate();
                }
                connection.commit();
                connection.close();
            }
            else
            {
                connection.commit();
                connection.close();
            }
        }
        catch (SQLException e)
        {
            e.printStackTrace();
        }
    }

    @Override
    public void addEnrolledCourseWithGrade(int studentId, int sectionId, @Nullable Grade grade) {
        try
        {
            Connection connection = SQLDataSource.getInstance().getSQLConnection();
            connection.setAutoCommit(false);
            connection.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
            PreparedStatement pst = null;
            ResultSet rst;
            String sql = "select semesterid from coursesection where id = ?";
            pst = (PreparedStatement) connection.prepareStatement(sql);
            pst.setInt(1, sectionId);
            rst = pst.executeQuery();
            int semesterid = 0;
            if (rst.next())
            {
                semesterid = rst.getInt(1);
            }
//            else
//            {
//                //connection.commit();
//                connection.close();
//                throw new IntegrityViolationException();
//            }
//            sql = "select courseid, grade, PF, semesterid from studentgrades where studentid = ? and sectionid = ?";
//            pst = (PreparedStatement) connection.prepareStatement(sql);
//            pst.setInt(1, studentId);
//            pst.setInt(2, sectionId);
//            rst = pst.executeQuery();
//            if (rst.next())
//            {
//                String courseid = rst.getString(1);
//                Object pf = rst.getObject(3);
//                int tmpsemester = rst.getInt(4);
//                if (pf != null)
//                {
//                    connection.commit();
//                    connection.close();
//                    throw new IntegrityViolationException();
//                }
//            }
            sql = "select course.id, course.grading from course join coursesection on "
                    + "course.id = coursesection.courseid "
                    + "where coursesection.id = ?";
            pst = (PreparedStatement) connection.prepareStatement(sql);
            pst.setInt(1, sectionId);
            rst = pst.executeQuery();
            if (rst.next())
            {
                sql = "insert into studentgrades (courseid, sectionid, studentid, grade, pf, semesterid) values(?,?,?,?,?,?)";
                pst = (PreparedStatement) connection.prepareStatement(sql);
                pst.setString(1, rst.getString(1));
                pst.setInt(2, sectionId);
                pst.setInt(3, studentId);
                if (grade != null)
                {
                    String grading = rst.getString(2);
                    if (grading.equals("HundredMarkScore"))
                    {
                        if (grade instanceof HundredMarkGrade)
                        {
                            pst.setInt(4, ((HundredMarkGrade) grade).mark);
                            if (((HundredMarkGrade) grade).mark >= 60)
                            {
                                pst.setInt(5, 1);
                            }
                            else
                            {
                                pst.setInt(5, 0);
                            }
                        }
                        else
                        {
                            //connection.commit();
                            connection.close();
                            throw new IntegrityViolationException();
                        }
                    }
                    else if (grading.equals("PassOrFail"))
                    {
                        if (grade == PassOrFailGrade.PASS)
                        {
                            pst.setInt(4, 60);
                            pst.setInt(5, 1);
                        }
                        else if (grade == PassOrFailGrade.FAIL)
                        {
                            pst.setInt(4, 0);
                            pst.setInt(5, 0);
                        }
                        else
                        {
                            //connection.commit();
                            connection.close();
                            throw new IntegrityViolationException();
                        }
                    }
                    else
                    {
                        pst.setObject(4, grade);
                        pst.setObject(5, grade);
                    }
                }
                else
                {
                    pst.setNull(4, java.sql.Types.INTEGER);
                    pst.setNull(5, java.sql.Types.INTEGER);
                }
                pst.setInt(6, semesterid);
                pst.executeUpdate();
                connection.commit();
                connection.close();
            }
//            else
//            {
//                connection.commit();
//                connection.close();
//                throw new IntegrityViolationException();
//            }
        }
        catch (SQLException e)
        {
            e.printStackTrace();
        }
    }

    @Override
    public void setEnrolledCourseGrade(int studentId, int sectionId, Grade grade) {
        String courseId = null;
        try {
            Connection connection = SQLDataSource.getInstance().getSQLConnection();
            connection.setAutoCommit(false);
            connection.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
            PreparedStatement pst1 = null;
            String sql1 = "select courseid from studentgrades where studentid=? and sectionid = ?";
            pst1 = (PreparedStatement) connection.prepareStatement(sql1);
            pst1.setInt(2, sectionId);
            pst1.setInt(1, studentId);
            ResultSet rst1 = pst1.executeQuery();
            if (rst1.next())
            {
                courseId=rst1.getString(1);
            }
            String sql = "select course.id, course.grading from course join coursesection on "
                    + "course.id = coursesection.courseid "
                    + "where coursesection.id = ?";
            PreparedStatement pst = (PreparedStatement) connection.prepareStatement(sql);
            pst.setInt(1, sectionId);
            ResultSet rst = pst.executeQuery();
            sql = "update studentGrades set grade=? , PF=? where studentId=? and sectionId=? and courseId=?;";
            pst = (PreparedStatement) connection.prepareStatement(sql);
            pst.setString(5, courseId);
            pst.setInt(4, sectionId);
            pst.setInt(3, studentId);
            String grading = rst.getString(2);
            if (grading.equals("HundredMarkScore"))
            {
                if (grade instanceof HundredMarkGrade)
                {
                    pst.setInt(4, ((HundredMarkGrade) grade).mark);
                    if (((HundredMarkGrade) grade).mark >= 60)
                    {
                        pst.setInt(5, 1);
                    }
                    else
                    {
                        pst.setInt(5, 0);
                    }
                }
            }
            else if (grading.equals("PassOrFail"))
            {
                if (grade == PassOrFailGrade.PASS)
                {
                    pst.setInt(4, 60);
                    pst.setInt(5, 1);
                }
                else if (grade == PassOrFailGrade.FAIL)
                {
                    pst.setInt(4, 0);
                    pst.setInt(5, 0);
                }
            }
            else
            {
                pst.setObject(4, grade);
                pst.setObject(5, grade);
            }
            pst.executeUpdate();
            connection.commit();
            connection.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Override
    public Map<Course, Grade> getEnrolledCoursesAndGrades(int studentId, @Nullable Integer semesterId) {
        try
        {
            Map<Course, Grade> m = new HashMap<Course, Grade>();
            Connection connection = SQLDataSource.getInstance().getSQLConnection();
            connection.setAutoCommit(false);
            connection.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
            PreparedStatement pst = null;
            String sql = "select count(*) from user1 where id = ? and type = 1";
            pst = (PreparedStatement) connection.prepareStatement(sql);
            pst.setInt(1, studentId);
            ResultSet rst = pst.executeQuery();
            if (rst.next())
            {
                int count = rst.getInt(1);
                if (count == 0)
                {
                    connection.commit();
                    connection.close();
                    throw new EntityNotFoundException();
                }
            }
            if (semesterId != null)
            {
                sql = "select count(*) from semester where id = ?";
                pst = (PreparedStatement) connection.prepareStatement(sql);
                pst.setInt(1, semesterId);
                rst = pst.executeQuery();
                if (rst.next())
                {
                    int count = rst.getInt(1);
                    if (count == 0)
                    {
                        connection.commit();
                        connection.close();
                        throw new EntityNotFoundException();
                    }
                }
                sql = "select studentgrades.grade, studentgrades.pf, course.grading from studentgrades "
                        + "join coursesection on coursesetion.id = studentgrades.sectionid "
                        + "join course on course.id = studentgrades.courseid "
                        + "where coursesection.semesterid = ? and studentgrades.studentid = ?";
                pst = (PreparedStatement) connection.prepareStatement(sql);
                pst.setInt(1, semesterId);
                pst.setInt(2, studentId);
            }
            else
            {
                sql = "select studentgrades.grade, studentgrades.pf, course.grading, "
                        + "course.id, course.name, course.credit, course.classhour "
                        + "from studentgrades "
                        + "join course on course.id = studentgrades.courseid "
                        + "where studentgrades.studentid = ?";
                pst = (PreparedStatement) connection.prepareStatement(sql);
                pst.setInt(1, studentId);
            }
            rst = pst.executeQuery();
            while(rst.next())
            {
                String grading = rst.getString(3);
                String courseid = rst.getString(4);
                String coursename = rst.getString(5);
                int credit = rst.getInt(6);
                int classhour = rst.getInt(7);
                Course course = new Course();
                course.id = courseid;
                course.name = coursename;
                course.credit = credit;
                course.classHour = classhour;
                course.grading = (grading.equals("HundredMarkScore")? Course.CourseGrading.HUNDRED_MARK_SCORE:Course.CourseGrading.PASS_OR_FAIL);
                if (grading.equals("HundredMarkScore"))
                {
                    Object mark = rst.getObject(1);
                    if (mark != null)
                    {
                        Grade grade = new HundredMarkGrade((short)mark);
                        m.put(course, grade);
                    }
                    else
                    {
                        m.put(course, null);
                    }
                }
                else if (grading.equals("PassOrFail"))
                {
                    Object mark = rst.getObject(2);
                    if (mark != null)
                    {
                        if ((int)mark == 1)
                        {
                            m.put(course, PassOrFailGrade.PASS);
                        }
                        else
                        {
                            m.put(course, PassOrFailGrade.FAIL);
                        }
                    }
                    else
                    {
                        m.put(course, null);
                    }
                }
            }
            connection.commit();
            connection.close();
            return m;
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public CourseTable getCourseTable(int studentId, Date date) {
        Map<DayOfWeek, Set<CourseTable.CourseTableEntry>> table = new HashMap<DayOfWeek, Set<CourseTable.CourseTableEntry>>();
        table.put(DayOfWeek.MONDAY, new HashSet<CourseTable.CourseTableEntry>());
        table.put(DayOfWeek.TUESDAY, new HashSet<CourseTable.CourseTableEntry>());
        table.put(DayOfWeek.WEDNESDAY, new HashSet<CourseTable.CourseTableEntry>());
        table.put(DayOfWeek.THURSDAY, new HashSet<CourseTable.CourseTableEntry>());
        table.put(DayOfWeek.FRIDAY, new HashSet<CourseTable.CourseTableEntry>());
        table.put(DayOfWeek.SATURDAY, new HashSet<CourseTable.CourseTableEntry>());
        table.put(DayOfWeek.SUNDAY, new HashSet<CourseTable.CourseTableEntry>());
        try
        {
            Connection connection = SQLDataSource.getInstance().getSQLConnection();
            connection.setAutoCommit(false);
            connection.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
            PreparedStatement pst = null;
            String sql;
            ResultSet rst;
            sql = "select id, begin from semester where ? between begin and end1";
            pst = (PreparedStatement) connection.prepareStatement(sql);
            pst.setDate(1, date);
            rst = pst.executeQuery();
            int week_num = 0;
            int semesterid = 0;
            if (rst.next())
            {
                semesterid = rst.getInt(1);
                Date begin = rst.getDate(2);
                SimpleDateFormat simpleFormat = new SimpleDateFormat("yyyy-MM-dd");
                java.util.Date fromDate1 = simpleFormat.parse(simpleFormat.format(begin));
                java.util.Date toDate1 = simpleFormat.parse(simpleFormat.format(date));
                long from1 = fromDate1.getTime();
                long to1 = toDate1.getTime();
                int days = (int) ((to1 - from1)/ (1000 * 60 * 60 * 24)) + 1;
                week_num = (days % 7 == 0 ? days / 7 : (days / 7 + 1));
            }
            sql = "select studentgrades.courseid, studentgrades.sectionid from studentgrades "
                    + "join coursesection on coursesection.id = studentgrades.sectionid "
                    + "where studentgrades.studentid = ? and coursesection.semesterid = ? ";
            //+ "and studentgrades.pf is null and studentgrades.grade is null";
            pst = (PreparedStatement) connection.prepareStatement(sql);
            pst.setInt(1, studentId);
            pst.setInt(2, semesterid);
            rst = pst.executeQuery();
            while (rst.next())
            {
                String courseid = rst.getString(1);
                int sectionid = rst.getInt(2);
                String subsql = "select course.name, coursesection.sectionname, "
                        + "user1.firstname, user1.lastname, user1.id, "
                        + "coursesectionclass.dayofweek, coursesectionclass.weeklist,"
                        + "coursesectionclass.classbegin, coursesectionclass.classend,"
                        + "coursesectionclass.location "
                        + "from "
                        + "course join coursesection on course.id = coursesection.courseid "
                        + "join coursesectionclass on coursesectionclass.coursesectionid = coursesection.id "
                        + "join user1 on coursesectionclass.instructor = user1.id "
                        + "where course.id = ? and coursesection.id = ?";
                PreparedStatement subpst = (PreparedStatement) connection.prepareStatement(subsql);
                subpst.setString(1, courseid);
                subpst.setInt(2, sectionid);
                ResultSet subrst = subpst.executeQuery();
                while (subrst.next())
                {
                    String coursename = subrst.getString(1);
                    String sectionname = subrst.getString(2);
                    String coursefullname = String.format("%s[%s]", coursename, sectionname);
                    String fullname;
                    String firstname = subrst.getString(3);
                    String lastname = subrst.getString(4);
                    if(firstname.matches("^[A-Z\\sa-z]+$") && lastname.matches("^[A-Z\\sa-z]+$"))
                        fullname = firstname + " " + lastname;
                    else
                        fullname = firstname + lastname;
                    int instructorid = subrst.getInt(5);
                    Instructor instructor = new Instructor();
                    instructor.id = instructorid;
                    instructor.fullName = fullname;
                    short classbegin = subrst.getShort(8);
                    short classend = subrst.getShort(9);
                    String location = subrst.getString(10);
                    int dayofweek = subrst.getInt(6);
                    Array array = subrst.getArray(7);
                    List<Short> weeklist = new ArrayList<Short>();
                    if (array != null)
                    {
                        ResultSet arr = array.getResultSet();
                        while (arr.next())
                        {
                            weeklist.add(arr.getShort(2));
                        }
                    }
                    for (short i : weeklist)
                    {
                        if (i == week_num)
                        {
                            CourseTable.CourseTableEntry entry = new CourseTable.CourseTableEntry();
                            entry.courseFullName = coursefullname;
                            entry.instructor = instructor;
                            entry.classBegin = classbegin;
                            entry.classEnd = classend;
                            entry.location = location;
                            switch (dayofweek)
                            {
                                case 1:
                                    table.get(DayOfWeek.MONDAY).add(entry);
                                    break;
                                case 2:
                                    table.get(DayOfWeek.TUESDAY).add(entry);
                                    break;
                                case 3:
                                    table.get(DayOfWeek.WEDNESDAY).add(entry);
                                    break;
                                case 4:
                                    table.get(DayOfWeek.THURSDAY).add(entry);
                                    break;
                                case 5:
                                    table.get(DayOfWeek.FRIDAY).add(entry);
                                    break;
                                case 6:
                                    table.get(DayOfWeek.SATURDAY).add(entry);
                                    break;
                                case 7:
                                    table.get(DayOfWeek.SUNDAY).add(entry);
                                    break;
                                default:
                            }
                        }
                    }
                }
            }
            CourseTable courseTable =  new CourseTable();
            courseTable.table = table;
            connection.commit();
            connection.close();
            return courseTable;
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public boolean passedPrerequisitesForCourse(int studentId, String courseId) {
        boolean res = false;
        try {
            int exist = 0;
            Connection connection = SQLDataSource.getInstance().getSQLConnection();
            connection.setAutoCommit(false);
            connection.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
            //判断StudentId合法
            String sql;
            PreparedStatement pst;
            ResultSet resultSet;
            //判断CourseId合法
            //获取所选课的Prerequisite
            String sql2 = "SELECT prerequisite from course where id = ?";
            pst = connection.prepareStatement(sql2);
            pst.setString(1,courseId);
            resultSet = pst.executeQuery();
            String str = null;
            if (resultSet.next()){
                str = resultSet.getString(1);
            }else{
                connection.commit();
                connection.close();
                return false;
            }
            Prerequisite prerequisite = StringToPrerequisite(str);
            //BFS开始
            //对于每层查询成绩
            res = checkLevel(prerequisite,connection,pst,resultSet,studentId);
            resultSet.close();
            pst.close();
            connection.commit();
            connection.close();
        }
        catch (Exception e){
            e.printStackTrace();
        }
        return res;
    }
    //对于每层查询成绩
    public static boolean checkLevel(Prerequisite prerequisite,Connection connection,PreparedStatement pst,ResultSet resultSet,int studentId){
        //
        if (prerequisite==null){
            return true;
        }
        //
        boolean judge = false;
        try {
            if (prerequisite instanceof CoursePrerequisite) {
                pst = connection.prepareStatement("select PF from studentGrades where studentid = ? and courseid  = ?");
                pst.setInt(1, studentId);
                pst.setString(2, ((CoursePrerequisite) prerequisite).courseID);
                resultSet = pst.executeQuery();
                int pass = 0;
                if (resultSet.next()) {
                    pass = resultSet.getInt(1);
                }
                return pass != 0;
            } else if (prerequisite instanceof AndPrerequisite) {
                judge = true;
                for (Prerequisite cur:((AndPrerequisite) prerequisite).terms){
                    judge = judge&&checkLevel(cur,connection,pst,resultSet,studentId);
                }
            } else if (prerequisite instanceof OrPrerequisite) {
                judge = false;
                for (Prerequisite cur:((OrPrerequisite) prerequisite).terms){
                    judge = judge||checkLevel(cur,connection,pst,resultSet,studentId);
                }
            }
        }catch (Exception e){
            e.printStackTrace();
        }
        return judge;
    }

    public static Prerequisite StringToPrerequisite(String str){
        //
        if(str==null){
            return null;
        }
        //
        str = str.trim();
        String handle = str.substring(1,str.length()-1);
        int len = handle.length();
        int hasLeft = 0;
        ArrayList<Integer> splitIndexes = new ArrayList<>();
        boolean and = false;
        boolean or = false;
        List<Prerequisite> sons = new ArrayList<>();
        for(int i = 0;i<len;i++ ){
            if (handle.charAt(i)=='('){
                hasLeft++;
            }
            if (handle.charAt(i)==')'){
                hasLeft--;
            }
            if (hasLeft==0&&(handle.charAt(i)=='+'||handle.charAt(i)=='*')){
                splitIndexes.add(i);
                if (handle.charAt(i)=='+'){
                    or = true;
                }else if (handle.charAt(i)=='*'){
                    and = true;
                }
            }
        }
        if (splitIndexes.size()==0){
            return new CoursePrerequisite(str);
        }
        splitIndexes.add(0,-1);
        handle+=" ";
        splitIndexes.add(handle.length()-1);
        Prerequisite prerequisite = null;
        if (or){
            prerequisite = new OrPrerequisite(sons);
            for (int i = 0;i<splitIndexes.size()-1;i++) {
                String next = handle.substring(splitIndexes.get(i)+1,splitIndexes.get(i+1));
                sons.add(StringToPrerequisite(next.trim()));
            }
        }
        if (and){
            prerequisite = new AndPrerequisite(sons);
            for (int i = 0;i<splitIndexes.size()-1;i++) {
                String next = handle.substring(splitIndexes.get(i)+1,splitIndexes.get(i+1));
                sons.add(StringToPrerequisite(next.trim()));
            }
        }
        return prerequisite;
    }

    @Override
    public Major getStudentMajor(int studentId) {
        int majorid = 0;
        try
        {
            Connection connection = SQLDataSource.getInstance().getSQLConnection();
            connection.setAutoCommit(false);
            connection.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
            PreparedStatement pst = null;
            String sql = "select count(*) from user1 where id = ? and type = 1";
            pst = (PreparedStatement) connection.prepareStatement(sql);
            pst.setInt(1, studentId);
            ResultSet rst = pst.executeQuery();
            if (rst.next())
            {
                int count = rst.getInt(1);
                if (count == 0)
                {
                    connection.commit();
                    connection.close();
                    throw new EntityNotFoundException();
                }
            }
            sql = "select count(*) from user1 where id = ? and type = 1";
            pst = (PreparedStatement) connection.prepareStatement(sql);
            pst.setInt(1, studentId);
            rst = pst.executeQuery();
            if(rst.next())
            {
                int count = rst.getInt(1);
                if (count == 0)
                {
                    connection.commit();
                    connection.close();
                    throw new EntityNotFoundException();
                }
            }
            sql = "select majorid from user1 where id = ? and type = 1";
            pst = (PreparedStatement) connection.prepareStatement(sql);
            pst.setInt(1, studentId);
            rst = pst.executeQuery();
            if (rst.next())
            {
                majorid = rst.getInt(1);
            }
            sql = "select major.id, major.name, department.id, department.name "
                    + "from major join department on major.departmentid = department.id "
                    + "where major.id = ?";
            pst = (PreparedStatement) connection.prepareStatement(sql);
            pst.setInt(1, majorid);
            rst = pst.executeQuery();
            if(rst.next())
            {
                String majorname = rst.getString(2);
                int departmentid = rst.getInt(3);
                String departmentname = rst.getString(4);
                Department department = new Department();
                department.id = departmentid;
                department.name = departmentname;
                Major major = new Major();
                major.id = majorid;
                major.name = majorname;
                major.department = department;
                connection.commit();
                connection.close();
                return major;
            }
            else
            {
                connection.commit();
                connection.close();
                throw new EntityNotFoundException();
            }
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
        return null;
    }
}

