package cn.edu.sustech.cs307.implementation;

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

import javax.annotation.Nullable;
import java.sql.*;
import java.sql.Date;
import java.text.SimpleDateFormat;
import java.time.DayOfWeek;
import java.util.*;

public class ReferenceStudentService implements StudentService {
    @Override
    public void addStudent(int userId, int majorId, String firstName, String lastName, Date enrolledDate) {
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
            String sql = "INSERT INTO student(id,firstname,lastname,enrolleddate,majorid) values(?,?,?,?,?)";
            pst = (PreparedStatement) connection.prepareStatement(sql);
            pst.setInt(1, userId);
            pst.setString(2, firstName);
            pst.setString(3, lastName);
            pst.setDate(4, enrolledDate);
            pst.setInt(5, majorId);
            pst.executeUpdate();
            sql = "INSERT INTO user1(id,firstname,lastname,role) values(?,?,?,?)";
            pst = (PreparedStatement) connection.prepareStatement(sql);
            pst.setInt(1, userId);
            pst.setString(2, firstName);
            pst.setString(3, lastName);
            pst.setInt(4, 1);
            pst.executeUpdate();
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    @Override
    public List<CourseSearchEntry> searchCourse(int studentId, int semesterId, @Nullable String searchCid, @Nullable String searchName, @Nullable String searchInstructor, @Nullable DayOfWeek searchDayOfWeek, @Nullable Short searchClassTime, @Nullable List<String> searchClassLocations, CourseType searchCourseType, boolean ignoreFull, boolean ignoreConflict, boolean ignorePassed, boolean ignoreMissingPrerequisites, int pageSize, int pageIndex) {
        try {
            Connection connection = SQLDataSource.getInstance().getSQLConnection();
            PreparedStatement pst = null;
            String sql = "select * from student where id = ?";
            pst = (PreparedStatement) connection.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
            pst.setInt(1, studentId);
            pst.executeUpdate();
            ResultSet rst = pst.getGeneratedKeys();
            if(!rst.next())
            {
                throw new EntityNotFoundException();
            }
            sql = "select * from semester where id = ?";
            pst = (PreparedStatement) connection.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
            pst.setInt(1, semesterId);
            pst.executeUpdate();
            rst = pst.getGeneratedKeys();
            if(!rst.next())
            {
                throw new EntityNotFoundException();
            }
            Major major = getStudentMajor(studentId);
            sql = "select course.id, course.name, course.credit, course.classhour, course.grading, "
                    + "coursesection.id, coursesection.sectionname, coursessection.totalcapacity, coursesection.leftcapacity "
                    + "coursesectionclass.dayofweek, coursesectionclass.weeklist, coursesectionclass.classbegin, "
                    + "coursesectionclass.classend, coursesectionclass.location "
                    + "from "
                    + "majorcourse join coursesection on majorcourse.courseid = coursesection.courseid "
                    + "join course on coursesection.courseid = course.id "
                    + "join coursesectionclass on coursesectionclass.coursesecitonid = coursesection.id "
                    + "join instructor on instructor.id = coursesectionclass.instructorid "
                    + "left join studentgrades on studentgrades.courseid = course.id "
                    + "where majorcourse.majorid = ? "
                    + "and studentgrades.studentid = ? "
                    + "and majorcourse.type = ? "
                    + "and coursesection.semesterid = ? ";
            if (searchCid != null)
            {
                sql = sql + "and coursesection.courseid = ? ";
            }
            if (searchName != null)
            {
                sql = sql + "and position(? in (course.name||'['||coursesection.name||']')) > 0 ";
            }
            if (searchInstructor != null)
            {
                String subq = "instructor.firstname||instructor.lastname like '?%'"
                        + "or instructor.firstname||' '||instructor.lastname like '?%'"
                        + "or instructor.firstname like '?%'"
                        + "or instructor.lastname like '?%'";
                sql = sql + "and (" + subq + ") ";
            }
            if (searchDayOfWeek != null)
            {
                sql = sql + "and ? in coursesectionclass.dayofweek ";
            }
            if (searchClassTime != null)
            {
                sql = sql + "and coursesectionclass.classbegin <= ? and coursesectionclass.classend >= ? ";
            }
            if (searchClassLocations != null)
            {
                int len = searchClassLocations.size();
                sql = sql + "and (";
                for (int i = 0; i < len - 1; i++)
                {
                    sql = sql + "? in coursesectionclass.location or ";
                }
                sql = sql + "? in coursesectionclass.location ) ";
            }
            if (ignoreFull)
            {
                sql = sql + "and coursesection.leftcapacity > 0 ";
            }
            if (ignorePassed)
            {
                sql = sql + "and studentgrades.courseid is not null "
                        + " and studentgrades.PF = 1 ";
            }
            pst = (PreparedStatement) connection.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
            int pos = 4;
            pst.setInt(1, major.id);
            pst.setInt(2, studentId);
            switch (searchCourseType)
            {
                case ALL:
                    pst.setInt(3, 0);
                    break;
                case MAJOR_COMPULSORY:
                    pst.setInt(3, 1);
                    break;
                case MAJOR_ELECTIVE:
                    pst.setInt(3, 2);
                    break;
                case CROSS_MAJOR:
                    pst.setInt(3, 3);
                    break;
                case PUBLIC:
                    pst.setInt(3, 4);
                    break;
            }
            pst.setInt(4, semesterId);
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
            }
            if (searchDayOfWeek != null)
            {
                pos++;
                pst.setObject(pos, searchDayOfWeek);
            }
            if (searchClassTime != null)
            {
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
            pst.executeUpdate();
            rst = pst.getGeneratedKeys();
            List<CourseSearchEntry> result = new ArrayList<CourseSearchEntry>();
            while(rst.next())
            {
                String courseid = rst.getString(1);
                String coursename = rst.getString(2);
                int credit = rst.getInt(3);
                int classhour = rst.getInt(4);
                String grading = rst.getString(5);
                if (ignoreMissingPrerequisites)
                {
                    if (!passedPrerequisitesForCourse(studentId, courseid))
                    {
                        continue;
                    }
                }
                Course course = new Course();
                course.id = courseid;
                course.name = coursename;
                course.credit = credit;
                course.classHour = classhour;
                course.grading = (grading.equals("HundredMarkGrade")? Course.CourseGrading.HUNDRED_MARK_SCORE:Course.CourseGrading.PASS_OR_FAIL);
                int sectionid = rst.getInt(6);
                String sectionname = rst.getString(7);
                int totalcapacity = rst.getInt(8);
                int leftcapacity = rst.getInt(9);
                List<String> conflictCourseNames = new ArrayList<String>();
                int dayofweek = rst.getInt(10);
                Array array = rst.getArray(11);
                Object o = array.getArray();
                Short[] weeks = (Short[]) o;
                List<Short> weeklist = List.of(weeks);
                short classbegin = rst.getShort(12);
                short classend = rst.getShort(13);
                String location = rst.getString(14);
                String subsql = "select coursesectionclass.dayofweek, coursesectionclass.weeklist, "
                        + "coursesectionclass.classbegin, coursesectionclass.classend, coursesectionclass.location, "
                        + "course.name, coursesection.sectionname, course.id "
                        + "from studentgrades join coursesection on studentgrades.sectionid = coursesection.id "
                        + "join course on studentgrades.courseid = course.id "
                        + "join coursesectionclass on coursesection.id = coursesectionclass.coursesectionid "
                        + "where studentgrades.studentid = ? and studentgrades.semesterid = ? and "
                        + "studentgrades.grade is null and studentgrades.pf is null";
                PreparedStatement subpst = (PreparedStatement) connection.prepareStatement(subsql, Statement.RETURN_GENERATED_KEYS);
                subpst.setInt(1, studentId);
                subpst.setInt(2, semesterId);
                subpst.executeUpdate();
                ResultSet subrst = subpst.getGeneratedKeys();
                boolean isconflict = false;
                while (subrst.next())
                {
                    if (subrst.getString(8).equals(courseid))
                    {
                        if (!ignoreConflict)
                            conflictCourseNames.add(String.format("%s[%s]", subrst.getString(6), subrst.getString(7)));
                        else
                            isconflict = true;
                    }
                    else
                    {
                        int tmpdayofweek = rst.getInt(1);
                        Array tmparray = rst.getArray(2);
                        Object tmpo = array.getArray();
                        Short[] tmpweeks = (Short[]) o;
                        List<Short> tmpweeklist = List.of(tmpweeks);
                        short tmpclassbegin = rst.getShort(3);
                        short tmpclassend = rst.getShort(4);
                        String tmplocation = rst.getString(5);
                        for (short i : weeklist)
                        {
                            for (short j : tmpweeklist)
                            {
                                if (i == j && dayofweek == tmpdayofweek &&
                                        ((classbegin <= tmpclassbegin && tmpclassbegin <= classend) ||
                                                (classbegin <= tmpclassend && tmpclassend <= classend)) &&
                                            (location.equals(tmplocation)))
                                {
                                    if (!ignoreConflict)
                                        conflictCourseNames.add(String.format("%s[%s]", subrst.getString(6), subrst.getString(7)));
                                    else
                                        isconflict = true;
                                }
                            }
                        }
                    }
                }
                if (!ignoreConflict)
                {
                    conflictCourseNames.sort(new Comparator<String>() {

                        @Override
                        public int compare(String o1, String o2) {
                            if (o1.equals(o2)) {
                                return 0;
                            } else {
                                return o1.compareTo(o2) > 0 ? 1 : -1;
                            }
                        }

                    });
                }
                if (isconflict)
                {
                    continue;
                }
                CourseSection coursesection = new CourseSection();
                coursesection.id = sectionid;
                coursesection.name = sectionname;
                coursesection.totalCapacity = totalcapacity;
                coursesection.leftCapacity = leftcapacity;
                subsql = "select coursesectionclass.id, coursesectionclass.dayofweek, coursesectionclass.weeklist, "
                        + "coursesectionclass.classbegin, coursesectionclass.classend, coursesectionclass.location, "
                        + "instructor.firstname, instructor.lastname, instructor.id "
                        + "from coursesectionclass join instructor on coursesectionclass.instructorid = instructor.id "
                        + "where coursesectionclass.sectionid = ?";
                subpst = null;
                subpst = (PreparedStatement) connection.prepareStatement(subsql, Statement.RETURN_GENERATED_KEYS);
                subpst.setInt(1, sectionid);
                subpst.executeUpdate();
                subrst = subpst.getGeneratedKeys();
                List<CourseSectionClass> sectionClasses = new ArrayList<CourseSectionClass>();
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
                    array = subrst.getArray(3);
                    o = array.getArray();
                    weeks = (Short[]) o;
                    weeklist = List.of(weeks);
                    classbegin = subrst.getShort(4);
                    classend = subrst.getShort(5);
                    location = subrst.getString(6);
                    String firstname = subrst.getString(7);
                    String lastname = subrst.getString(8);
                    String fullname;
                    if(firstname.matches("^[A-Za-z]+$") && lastname.matches("^[A-Za-z]+$"))
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
                CourseSearchEntry courseSearchEntry = new CourseSearchEntry();
                courseSearchEntry.course = course;
                courseSearchEntry.section = coursesection;
                courseSearchEntry.sectionClasses = new HashSet<>(sectionClasses);
                if (!ignoreConflict)
                {
                    courseSearchEntry.conflictCourseNames = conflictCourseNames;
                }
                else
                {
                    courseSearchEntry.conflictCourseNames = List.of();
                }
                result.add(courseSearchEntry);
            }
            result.sort(new Comparator<CourseSearchEntry>() {

                @Override
                public int compare(CourseSearchEntry o1, CourseSearchEntry o2) {
                    String id1 = o1.course.id;
                    String id2 = o2.course.id;
                    if (id1.equals(id2)) {
                        String name1 = String.format("%s[%s]", o1.course.name, o1.section.name);
                        String name2 = String.format("%s[%s]", o2.course.name, o2.section.name);
                        if (name1.equals(name2)) {
                            return 0;
                        } else {
                            return name1.compareTo(name2) > 0 ? 1 : -1;
                        }
                    } else {
                        return id1.compareTo(id2) > 0 ? 1 : -1;
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
            PreparedStatement pst = null;
            String sql = "select * from coursesection where id = ?";
            pst = (PreparedStatement) connection.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
            pst.setInt(1, sectionId);
            pst.executeUpdate();
            ResultSet rst = pst.getGeneratedKeys();
            if (!rst.next())
            {
                return EnrollResult.COURSE_NOT_FOUND;
            }
            sql = "select semesterid from coursesection where id = ?";
            pst = (PreparedStatement) connection.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
            pst.setInt(1, sectionId);
            pst.executeUpdate();
            rst = pst.getGeneratedKeys();
            int semesterid;
            if (!rst.next())
            {
                return EnrollResult.COURSE_NOT_FOUND;
            }
            else
            {
                semesterid = rst.getInt(1);
            }
            sql = "select courseid, grade, PF, semesterid from studentgrades where studentid = ? and sectionid = ?";
            pst = (PreparedStatement) connection.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
            pst.setInt(1, studentId);
            pst.setInt(2, sectionId);
            pst.executeUpdate();
            rst = pst.getGeneratedKeys();
            if (rst.next())
            {
                String courseid = rst.getString(1);
                Object grade = rst.getObject(2);
                Object pf = rst.getObject(3);
                int tmpsemester = rst.getInt(4);
                if (pf == null && tmpsemester == semesterid)
                {
                    return EnrollResult.ALREADY_ENROLLED;
                }
                else if (pf != null && (int)pf == 1)
                {
                    return EnrollResult.ALREADY_PASSED;
                }
                else
                {
                    return EnrollResult.UNKNOWN_ERROR;
                }
            }
            sql = "select coursesection.courseid, coursesection.leftcapacity, "
                    + "coursesectionclass.dayofweek, coursesectionclass.weeklist, "
                    + "coursesectionclass.classbegin, coursesectionclass.classend, "
                    + "coursesectionclass.location "
                    + "from coursesection "
                    + "join coursesectionclass on coursesection.id = coursesectionclass.coursesecitonid "
                    + "where coursesection.id = ?";
            pst = (PreparedStatement) connection.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
            pst.setInt(1, sectionId);
            pst.executeUpdate();
            rst = pst.getGeneratedKeys();
            String courseid;
            int leftcapacity, dayofweek;
            short classbegin, classend;
            Short[] weeks;
            List<Short> weeklist;
            String location;
            if (rst.next())
            {
                courseid = rst.getString(1);
                leftcapacity = rst.getInt(2);
                dayofweek = rst.getInt(3);
                Array array = rst.getArray(4);
                Object o = array.getArray();
                weeks = (Short[]) o;
                weeklist = List.of(weeks);
                classbegin = rst.getShort(5);
                classend = rst.getShort(6);
                location = rst.getString(7);
            }
            else
            {
                return EnrollResult.UNKNOWN_ERROR;
            }
            if (!passedPrerequisitesForCourse(studentId, courseid))
            {
                return EnrollResult.PREREQUISITES_NOT_FULFILLED;
            }
            sql = "select coursesection.courseid, coursesection.leftcapacity, "
                    + "coursesectionclass.dayofweek, coursesectionclass.weeklist, "
                    + "coursesectionclass.classbegin, coursesectionclass.classend, "
                    + "coursesectionclass.location "
                    + "from coursesection "
                    + "join coursesectionclass on coursesection.id = coursesectionclass.coursesecitonid "
                    + "join studentgrades on studentgrades.sectionid = coursesection.id "
                    + "where studentgrades.studentid = ? "
                    + "and studentgrades.PF is null and studentgrades.semesterid = ?";
            pst = (PreparedStatement) connection.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
            pst.setInt(1, studentId);
            pst.setInt(2, semesterid);
            pst.executeUpdate();
            rst = pst.getGeneratedKeys();
            while(rst.next())
            {
                String tmpcourseid = rst.getString(1);
                int tmpleftcapacity = rst.getInt(2);
                int tmpdayofweek = rst.getInt(3);
                Array array = rst.getArray(4);
                Object o = array.getArray();
                Short[] tmpweeks = (Short[]) o;
                List<Short> tmpweeklist = List.of(tmpweeks);
                short tmpclassbegin = rst.getShort(5);
                short tmpclassend = rst.getShort(6);
                String tmplocation = rst.getString(7);
                if (tmpcourseid.equals(courseid))
                {
                    return EnrollResult.COURSE_CONFLICT_FOUND;
                }
                for (short i : weeklist)
                {
                    for (short j : tmpweeklist)
                    {
                        if (i == j && dayofweek == tmpdayofweek &&
                                ((classbegin <= tmpclassbegin && tmpclassbegin <= classend) ||
                                        (classbegin <= tmpclassend && tmpclassend <= classend)) &&
                                (location.equals(tmplocation)))
                        {
                            return EnrollResult.COURSE_CONFLICT_FOUND;
                        }
                    }
                }
            }
            if (leftcapacity == 0)
            {
                return EnrollResult.COURSE_IS_FULL;
            }
            sql = "update coursesection set leftcapcity = ? where id = ?";
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
            PreparedStatement pst = null;
            String sql = "select grade, pf from studentgrades where studentid = ? and sectionid = ?";
            pst = (PreparedStatement) connection.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
            pst.setInt(1, studentId);
            pst.setInt(2, sectionId);
            pst.executeUpdate();
            ResultSet rst = pst.getGeneratedKeys();
            if (rst.next())
            {
                Object grade = rst.getObject(1);
                Object pf = rst.getObject(2);
                if (grade != null || pf != null)
                {
                    throw new IllegalStateException();
                }
                sql = "delete from studentgrades where studentid = ? and sectionid = ?";
                pst = (PreparedStatement) connection.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
                pst.setInt(1, studentId);
                pst.setInt(2, sectionId);
                pst.executeUpdate();
                sql = "select leftcapacity from coursesection where id = ?";
                pst = (PreparedStatement) connection.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
                pst.setInt(1, sectionId);
                pst.executeUpdate();
                if (rst.next())
                {
                    int leftcapacity = rst.getInt(1);
                    sql = "update coursesection set leftcapcity = ? where id = ?";
                    pst = (PreparedStatement) connection.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
                    pst.setInt(1, leftcapacity + 1);
                    pst.setInt(2, sectionId);
                    pst.executeUpdate();
                }
            }
            else
            {
                throw new EntityNotFoundException();
                // don't know what to do
            }
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
    }

    @Override
    public void addEnrolledCourseWithGrade(int studentId, int sectionId, @Nullable Grade grade) {
        try
        {
            Connection connection = SQLDataSource.getInstance().getSQLConnection();
            PreparedStatement pst = null;
            String sql = "select * from student where id = ?";
            pst = (PreparedStatement) connection.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
            pst.setInt(1, studentId);
            pst.executeUpdate();
            ResultSet rst = pst.getGeneratedKeys();
            if (!rst.next())
            {
                throw new IntegrityViolationException();
            }
            sql = "select semesterid from coursesection where id = ?";
            pst = (PreparedStatement) connection.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
            pst.setInt(1, sectionId);
            pst.executeUpdate();
            rst = pst.getGeneratedKeys();
            int semesterid;
            if (!rst.next())
            {
                throw new IntegrityViolationException();
            }
            else
            {
                semesterid = rst.getInt(1);
            }
            sql = "select courseid, grade, PF, semesterid from studentgrades where studentid = ? and sectionid = ?";
            pst = (PreparedStatement) connection.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
            pst.setInt(1, studentId);
            pst.setInt(2, sectionId);
            pst.executeUpdate();
            rst = pst.getGeneratedKeys();
            if (rst.next())
            {
                String courseid = rst.getString(1);
                Object pf = rst.getObject(3);
                int tmpsemester = rst.getInt(4);
                if (pf == null && tmpsemester == semesterid)
                {
                    throw new IntegrityViolationException();
                }
                else if (pf != null && (int)pf == 1)
                {
                    throw new IntegrityViolationException();
                }
            }
            sql = "select course.id, course.grading from course join coursesection on "
                    + "course.id = coursesection.courseid "
                    + "where coursesection.id = ?";
            pst = (PreparedStatement) connection.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
            pst.setInt(1, sectionId);
            pst.executeUpdate();
            rst = pst.getGeneratedKeys();
            if (rst.next())
            {
                sql = "insert into studentgrades (courseid, sectionid, studentid, grade, pf) values(?,?,?,?,?)";
                pst = (PreparedStatement) connection.prepareStatement(sql);
                pst.setString(1, rst.getString(1));
                pst.setInt(2, sectionId);
                pst.setInt(3, studentId);
                if (grade != null)
                {
                    String grading = rst.getString(2);
                    if (grading.equals("HundredMarkGrade"))
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
                            throw new IntegrityViolationException();
                        }
                    }
                    else if (grading.equals("PassOrFailGrade"))
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
            }
            else
            {
                throw new IntegrityViolationException();
            }
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
    }

    @Override
    public void setEnrolledCourseGrade(int studentId, int sectionId, Grade grade) {
        String courseId;
        try {
            Connection connection = SQLDataSource.getInstance().getSQLConnection();
            PreparedStatement pst2 = null;
            String sql2 = "select * from student where studentid = ?";
            pst2 = (PreparedStatement) connection.prepareStatement(sql2, Statement.RETURN_GENERATED_KEYS);
            pst2.setInt(1, studentId);
            pst2.executeUpdate();
            ResultSet rst2 = pst2.getGeneratedKeys();
            if (!rst2.next())
            {
                throw new IntegrityViolationException();
            }
            PreparedStatement pst1 = null;
            String sql1 = "select courseid from studentgrades where studentid=? and sectionid = ?";
            pst1 = (PreparedStatement) connection.prepareStatement(sql1, Statement.RETURN_GENERATED_KEYS);
            pst1.setInt(2, sectionId);
            pst1.setInt(1, studentId);
            pst1.executeUpdate();
            ResultSet rst1 = pst1.getGeneratedKeys();
            if (!rst1.next())
            {
                throw new IntegrityViolationException();
            }
            courseId=rst1.getString(1);
            String sql = "select course.id, course.grading from course join coursesection on "
                    + "course.id = coursesection.courseid "
                    + "where coursesection.id = ?";
            PreparedStatement pst = (PreparedStatement) connection.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
            pst.setInt(1, sectionId);
            pst.executeUpdate();
            ResultSet rst = pst.getGeneratedKeys();
            sql = "update studentGrades set grade=? , PF=? where studentId=? and sectionId=? and courseId=?;";
            pst = (PreparedStatement) connection.prepareStatement(sql);
            pst.setString(5,courseId );
            pst.setInt(4, sectionId);
            pst.setInt(3, studentId);
            String grading = rst.getString(2);
            if (grading.equals("HundredMarkGrade"))
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
                    throw new IntegrityViolationException();
                }
            }
            else if (grading.equals("PassOrFailGrade"))
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
                    throw new IntegrityViolationException();
                }
            }
            else
            {
                pst.setObject(4, grade);
                pst.setObject(5, grade);
            }
            pst.executeUpdate();
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
            PreparedStatement pst = null;
            String sql = "select * from student where id = ?";
            pst = (PreparedStatement) connection.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
            pst.setInt(1, studentId);
            pst.executeUpdate();
            ResultSet rst = pst.getGeneratedKeys();
            if (!rst.next())
            {
                throw new EntityNotFoundException();
            }
            if (semesterId != null)
            {
                sql = "select * from semester where id = ?";
                pst = (PreparedStatement) connection.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
                pst.setInt(1, semesterId);
                pst.executeUpdate();
                rst = pst.getGeneratedKeys();
                if (!rst.next())
                {
                    throw new EntityNotFoundException();
                }
                sql = "select studentgrades.grade, studentgrades.pf, course.grading from studentgrades "
                        + "join coursesection on coursesetion.id = studentgrades.sectionid "
                        + "join course on course.id = studentgrades.courseid "
                        + "where coursesection.semesterid = ? and studentgrades.studentid = ?";
                pst = (PreparedStatement) connection.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
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
                pst = (PreparedStatement) connection.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
                pst.setInt(1, studentId);
            }
            pst.executeUpdate();
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
                course.grading = (grading.equals("HundredMarkGrade")? Course.CourseGrading.HUNDRED_MARK_SCORE:Course.CourseGrading.PASS_OR_FAIL);
                if (grading.equals("HundredMarkGrade"))
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
                else if (grading.equals("PassOrFailGrade"))
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
            PreparedStatement pst = null;
            String sql = "select * from student where id = ?";
            pst = (PreparedStatement) connection.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
            pst.setInt(1, studentId);
            pst.executeUpdate();
            ResultSet rst = pst.getGeneratedKeys();
            if (!rst.next())
            {
                throw new EntityNotFoundException();
            }
            sql = "select id, begin from semester where ? between begin and end1";
            pst = (PreparedStatement) connection.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
            pst.setDate(1, date);
            pst.executeUpdate();
            rst = pst.getGeneratedKeys();
            int week_num;
            int semesterid;
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
            else
            {
                throw new EntityNotFoundException();
            }
            sql = "select studentgrades.courseid, studentgrades.sectionid from studentgrades "
                    + "join coursesection on coursesection.id = studentgrades.sectionid "
                    + "where studentgrades.studentid = ? and studentgrades.grade is null "
                    + "and studentgrades.pf is null and coursesection.semesterid = ?";
            pst = (PreparedStatement) connection.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
            pst.setInt(1, studentId);
            pst.setInt(2, semesterid);
            pst.executeUpdate();
            rst = pst.getGeneratedKeys();
            while (rst.next())
            {
                String courseid = rst.getString(1);
                int sectionid = rst.getInt(2);
                String subsql = "select course.name, coursesection.sectionname, "
                        + "instructor.firstname, instructor.lastname, instructor.id, "
                        + "coursesectionclass.dayofweek, coursesectionclass.weeklist,"
                        + "coursesectionclass.classbegin, coursesectionclass.classend,"
                        + "coursesectionclass.location "
                        + "from "
                        + "course join coursesection on course.id = coursesection.courseid "
                        + "join coursesectionclass on coursesectionclass.coursesectionid = coursesection.id "
                        + "join instructor on coursesectionclass.instructorid = instructor.id "
                        + "where course.id = ? and coursesection.id = ?";
                PreparedStatement subpst = (PreparedStatement) connection.prepareStatement(subsql, Statement.RETURN_GENERATED_KEYS);
                subpst.setString(1, courseid);
                subpst.setInt(2, sectionid);
                subpst.executeUpdate();
                ResultSet subrst = subpst.getGeneratedKeys();
                while (subrst.next())
                {
                    String coursename = subrst.getString(1);
                    String sectionname = subrst.getString(2);
                    String coursefullname = String.format("%s[%s]", coursename, sectionname);
                    String fullname;
                    String firstname = subrst.getString(3);
                    String lastname = subrst.getString(4);
                    if(firstname.matches("^[A-Za-z]+$") && lastname.matches("^[A-Za-z]+$"))
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
                    Object o = array.getArray();
                    Short[] weeks = (Short[]) o;
                    List<Short> weeklist = List.of(weeks);
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
            //判断StudentId合法
            String sql="SELECT count(*) from student where id = ?";
            PreparedStatement pst = connection.prepareStatement(sql);
            pst.setInt(1,studentId);
            ResultSet resultSet = pst.executeQuery();
            if (resultSet.next()) {
                exist = resultSet.getInt(1);
            }
            if (exist==0){
                throw new EntityNotFoundException();
            }
            //判断CourseId合法
            String sql1="SELECT count(*) from course where id = ?";
            pst = connection.prepareStatement(sql1);
            pst.setString(1,courseId);
            resultSet = pst.executeQuery();
            if (resultSet.next()) {
                exist = resultSet.getInt(1);
            }
            if (exist==0){
                throw new EntityNotFoundException();
            }
            //获取所选课的Prerequisite
            String sql2 = "SELECT prerequisite from course where id = ?";
            pst = connection.prepareStatement(sql2);
            pst.setString(1,courseId);
            resultSet = pst.executeQuery();
            String str = null;
            if (resultSet.next()){
                str = resultSet.getString(1);
            }else{
                return false;
            }
            Prerequisite prerequisite = StringToPrerequisite(str);
            //BFS开始
            //对于每层查询成绩
            res = checkLevel(prerequisite,connection,pst,resultSet,studentId);
            resultSet.close();
            pst.close();
            connection.close();

        }catch (Exception e){
            e.printStackTrace();
        }
        return res;
    }
    //对于每层查询成绩
    public static boolean checkLevel(Prerequisite prerequisite,Connection connection,PreparedStatement pst,ResultSet resultSet,int studentId){
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
        int majorid;
        try
        {
            Connection connection = SQLDataSource.getInstance().getSQLConnection();
            PreparedStatement pst = null;
            String sql = "select * from student where id = ?";
            pst = (PreparedStatement) connection.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
            pst.setInt(1, studentId);
            pst.executeUpdate();
            ResultSet rst = pst.getGeneratedKeys();
            if(!rst.next())
            {
                throw new EntityNotFoundException();
            }
            sql = "select majorid from student where id = ?";
            pst = (PreparedStatement) connection.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
            pst.setInt(1, studentId);
            pst.executeUpdate();
            rst = pst.getGeneratedKeys();
            if(rst.next())
            {
                majorid = rst.getInt(1);
            }
            else
            {
                throw new EntityNotFoundException();
            }
            sql = "select major.id, major.name, department.id, department.name "
                    + "from major join department on major.departmentid = department.id "
                    + "where major.id = ?";
            pst = (PreparedStatement) connection.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
            pst.setInt(1, majorid);
            pst.executeUpdate();
            rst = pst.getGeneratedKeys();
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
                return major;
            }
            else
            {
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