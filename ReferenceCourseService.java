package Implementation;
import cn.edu.sustech.cs307.database.SQLDataSource;
import cn.edu.sustech.cs307.dto.*;

import java.sql.Date;
import java.util.*;

import cn.edu.sustech.cs307.dto.prerequisite.AndPrerequisite;
import cn.edu.sustech.cs307.dto.prerequisite.CoursePrerequisite;
import cn.edu.sustech.cs307.dto.prerequisite.OrPrerequisite;
import cn.edu.sustech.cs307.dto.prerequisite.Prerequisite;
import cn.edu.sustech.cs307.exception.EntityNotFoundException;
import cn.edu.sustech.cs307.exception.IntegrityViolationException;
import cn.edu.sustech.cs307.service.CourseService;

import javax.annotation.Nullable;
import java.sql.*;
import java.time.DayOfWeek;

public class ReferenceCourseService implements CourseService {
    public static String getLogicPair(Prerequisite prerequisite){
        if (prerequisite==null){
            return null;
        }
        String expression = prerequisite.when(new Prerequisite.Cases<>() {
            @Override
            public String match(AndPrerequisite self) {
                String[] children = self.terms.stream()
                        .map(term -> term.when(this))
                        .toArray(String[]::new);
                return '(' + String.join(" * ", children) + ')';
            }

            @Override
            public String match(OrPrerequisite self) {
                String[] children = self.terms.stream()
                        .map(term -> term.when(this))
                        .toArray(String[]::new);
                return '(' + String.join(" + ", children) + ')';
            }

            @Override
            public String match(CoursePrerequisite self) {
                return self.courseID;
            }
        });
        return expression;
    }

    @Override
    public void addCourse(String courseId, String courseName, int credit, int classHour, Course.CourseGrading grading, @Nullable Prerequisite prerequisite) {
        try{
            //或许需要修改，额外插入一个select语句
            if (credit<0){
                throw new IntegrityViolationException();
            }
            //同上
            if (classHour<0){
                throw new IntegrityViolationException();
            }
            String sql="INSERT INTO Course(id,name,credit,classHour,grading,prerequisite) values(?,?,?,?,?,?)";
            //Statement.RETURN_GENERATED_KEYS:获取自动增加的id号
            Connection connection = SQLDataSource.getInstance().getSQLConnection();
            PreparedStatement pst = null;
            pst = (PreparedStatement) connection.prepareStatement(sql);
            //把相应的参数 添加到pst对象
            pst.setString(1,courseId.trim());
            pst.setString(2,courseName.trim());
            pst.setInt(3,credit);
            pst.setInt(4,classHour);
            String grade = "";
            if (grading == Course.CourseGrading.HUNDRED_MARK_SCORE){
                grade = "HundredMarkScore";
            }else if (grading == Course.CourseGrading.PASS_OR_FAIL){
                grade = "PassOrFail";
            }
            pst.setString(5,grade);
            pst.setString(6,getLogicPair(prerequisite));
            //提交pst对象
            pst.executeUpdate();
            connection.close();
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    @Override
    public int addCourseSection(String courseId, int semesterId, String sectionName, int totalCapacity) {
        Integer enterInfoId = 0;
        try{
            //是否自增？
//            if (semesterId<=0){
//                throw new IntegrityViolationException();
//            }
//            if(totalCapacity<=0){
//                throw new IntegrityViolationException();
//            }
            String sql="INSERT INTO CourseSection(courseId,semesterId,SectionName,totalCapacity,leftCapacity) values(?,?,?,?,?)";
            //Statement.RETURN_GENERATED_KEYS:获取自动增加的id号
            Connection connection = SQLDataSource.getInstance().getSQLConnection();
            PreparedStatement pst = null;
            pst = (PreparedStatement) connection.prepareStatement(sql,Statement.RETURN_GENERATED_KEYS);
            //把相应的参数 添加到pst对象
            pst.setString(1,courseId.trim());
            pst.setInt(2,semesterId);
            pst.setString(3,sectionName);
            pst.setInt(4,totalCapacity);
            pst.setInt(5,totalCapacity);
            pst.executeUpdate();
            ResultSet rst = pst.getGeneratedKeys();
            if(rst.next()) {
                enterInfoId = rst.getInt(1);
            }
            connection.close();
        }catch (Exception e){
            e.printStackTrace();
        }
        return enterInfoId;
    }

    @Override
    public int addCourseSectionClass(int sectionId, int instructorId, DayOfWeek dayOfWeek, Set<Short> weekList, short classStart, short classEnd, String location) {
        Integer enterInfoId = 0;
        try{
            //记得要有外键连接，保证不存在时会报错，待改
        if (sectionId<=0){
            throw new IntegrityViolationException();
        }
           //记得要有外键连接，保证不存在时报错，待改
        if (instructorId<=0){
            throw new IntegrityViolationException();
        }
        if (weekList.isEmpty()){
            throw new IntegrityViolationException();
        }
//        if (classStart <= 0 || classEnd <= 0 || classEnd > 10 || classStart > classEnd){
//            throw new IntegrityViolationException();
//        }
           String sql="INSERT INTO CourseSectionClass(CourseSectionId,instructor,dayofweek,weeklist,classBegin,classEnd,location) values(?,?,?,?,?,?,?)";
           //Statement.RETURN_GENERATED_KEYS:获取自动增加的id号
            Connection connection = SQLDataSource.getInstance().getSQLConnection();
            PreparedStatement pst = null;
            pst = (PreparedStatement) connection.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
           //把相应的参数 添加到pst对象
            int day = 0;
           if (dayOfWeek==DayOfWeek.MONDAY){
               day = 1;
           }else if(dayOfWeek==DayOfWeek.TUESDAY){
               day = 2;
           }else if (dayOfWeek==DayOfWeek.WEDNESDAY){
               day = 3;
           }else if (dayOfWeek==DayOfWeek.THURSDAY){
               day = 4;
           }else if (dayOfWeek==DayOfWeek.FRIDAY){
               day = 5;
           }else if (dayOfWeek==DayOfWeek.SATURDAY){
               day = 6;
           }else if (dayOfWeek==DayOfWeek.SUNDAY){
               day = 7;
           }else {
               connection.close();
               throw new IntegrityViolationException();
           }
           java.sql.Array sqlArray = connection.createArrayOf("int",weekList.toArray(new Short[0]));
           pst.setInt(1,sectionId);
           pst.setInt(2,instructorId);
           pst.setInt(3,day);
           pst.setArray(4,sqlArray);
           pst.setInt(5,classStart);
           pst.setInt(6,classEnd);
           pst.setString(7,location);
           //提交pst对象
           pst.executeUpdate();
           ResultSet rst = pst.getGeneratedKeys();
           if(rst.next()) {
               enterInfoId = rst.getInt(1);
           }
           connection.close();
       }catch (Exception e){
           e.printStackTrace();
       }
        return enterInfoId;
    }

    @Override
    public void removeCourse(String courseId) {
        try {
            int exist = 0;
            Connection connection = SQLDataSource.getInstance().getSQLConnection();
            connection.setAutoCommit(false);
            connection.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
            String sql="SELECT count(*) from course where id = ?";
            PreparedStatement pst = connection.prepareStatement(sql);
            pst.setString(1,courseId);
            ResultSet resultSet = pst.executeQuery();
            if (resultSet.next()){
                 exist = resultSet.getInt(1);
            }
            if (exist>0) {
                String sql1 = "DELETE FROM studentGrades where courseId = ? ";
                pst = connection.prepareStatement(sql1);
                pst.setString(1, courseId);
                pst.executeUpdate();
                String sql2 = "DELETE FROM courseSectionClass where coursesectionid in (select id from coursesection where courseid = ?)";
                pst = connection.prepareStatement(sql2);
                pst.setString(1, courseId);
                pst.executeUpdate();
                String sql3 = "DELETE FROM courseSection where courseid = ?";
                pst = connection.prepareStatement(sql3);
                pst.setString(1, courseId);
                pst.executeUpdate();
                String sql4 = "DELETE FROM course where id = ?";
                pst = connection.prepareStatement(sql4);
                pst.setString(1, courseId);
                pst.executeUpdate();
            }else {
                connection.commit();
                connection.close();
                throw new EntityNotFoundException();
            }
            connection.commit();
            connection.close();

        }catch (Exception e){
            e.printStackTrace();
        }

    }

    @Override
    public void removeCourseSection(int sectionId) {
        try {
            int exist = 0;
            Connection connection = SQLDataSource.getInstance().getSQLConnection();
            connection.setAutoCommit(false);
            connection.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
            String sql="SELECT count(*) from courseSection where id = ?";
            PreparedStatement pst = connection.prepareStatement(sql);
            pst.setInt(1,sectionId);
            ResultSet resultSet = pst.executeQuery();
            if (resultSet.next()) {
                exist = resultSet.getInt(1);
            }
            if (exist>0) {
                String sql1 = "DELETE FROM studentGrades where sectionId = ? ";
                pst = connection.prepareStatement(sql1);
                pst.setInt(1, sectionId);
                pst.executeUpdate();
                String sql2 = "DELETE FROM courseSectionClass where coursesectionid = ?";
                pst = connection.prepareStatement(sql2);
                pst.setInt(1, sectionId);
                pst.executeUpdate();
                String sql3 = "DELETE FROM courseSection where id = ?";
                pst = connection.prepareStatement(sql3);
                pst.setInt(1, sectionId);
                pst.executeUpdate();
            }else {
                connection.commit();
                connection.close();
                throw new EntityNotFoundException();
            }
            connection.commit();
            connection.close();

        }catch (Exception e){
            e.printStackTrace();
        }
    }

    @Override
    public void removeCourseSectionClass(int classId) {
        try {
            int exist = 0;
            Connection connection = SQLDataSource.getInstance().getSQLConnection();
            connection.setAutoCommit(false);
            connection.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
            String sql="SELECT count(*) from courseSectionClass where id = ?";
            PreparedStatement pst = connection.prepareStatement(sql);
            pst.setInt(1,classId);
            ResultSet resultSet = pst.executeQuery();
            if (resultSet.next()) {
                exist = resultSet.getInt(1);
            }
            if (exist>0) {
                connection = SQLDataSource.getInstance().getSQLConnection();
                String sql1 = "DELETE FROM courseSectionClass where id = ?";
                pst = connection.prepareStatement(sql1);
                pst.setInt(1, classId);
                pst.executeUpdate();
            }else{
                connection.commit();
                connection.close();
                throw new EntityNotFoundException();
            }
            connection.commit();
            connection.close();
        }catch (Exception e){
            e.printStackTrace();
        }
    }
    @Override
    public List<Course> getAllCourses() {
        List<Course> courseList = new ArrayList<Course>();
        try {
            Connection connection = SQLDataSource.getInstance().getSQLConnection();
            connection.setAutoCommit(false);
            connection.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
            String sql = "select * from course";
            PreparedStatement pst = connection.prepareStatement(sql);
            ResultSet resultSet =  pst.executeQuery();
            while (resultSet.next()) {
                String id = resultSet.getString(1);
                String courseName = resultSet.getString(2);
                int credit = resultSet.getInt(3);
                int classHour = resultSet.getInt(4);
                String grade = resultSet.getString(5);
                Course.CourseGrading grading = Course.CourseGrading.HUNDRED_MARK_SCORE;
                if (grade.equals("HundredMarkScore")) {
                    grading = Course.CourseGrading.HUNDRED_MARK_SCORE;
                } else if (grade.equals("PassOrFail")) {
                    grading = Course.CourseGrading.PASS_OR_FAIL;
                }
                Course cur = new Course();
                cur.id = id;
                cur.name = courseName;
                cur.credit = credit;
                cur.classHour = classHour;
                cur.grading = grading;
                courseList.add(cur);
            }
            connection.commit();
            connection.close();
        }catch (Exception e){
            e.printStackTrace();
        }
        return courseList;
    }

    @Override
    public List<CourseSection> getCourseSectionsInSemester(String courseId, int semesterId) {
        List<CourseSection> courseSectionList = new ArrayList<CourseSection>();
        try {
            Connection connection = SQLDataSource.getInstance().getSQLConnection();
            connection.setAutoCommit(false);
            connection.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
            String sql = "select count(*) from course where id = ? ";
            PreparedStatement pst = connection.prepareStatement(sql);
            pst.setString(1,courseId);
            ResultSet resultSet =  pst.executeQuery();
            int exist = 0;
            if (resultSet.next()){
                exist = resultSet.getInt(1);
            }
            if (exist==0){
                connection.commit();
                connection.close();
                throw new EntityNotFoundException();
            }

            String sql1 = "select count(*) from semester where id = ? ";
            pst = connection.prepareStatement(sql1);
            pst.setInt(1,semesterId);
            exist = 0;
            resultSet =  pst.executeQuery();
            if (resultSet.next()){
                exist = resultSet.getInt(1);
            }
            if (exist==0){
                connection.commit();
                connection.close();
                throw new EntityNotFoundException();
            }

            String sql2 = "select * from courseSection where courseId = ? and semesterId = ? ";
            pst = connection.prepareStatement(sql2);
            pst.setString(1,courseId);
            pst.setInt(2,semesterId);
            resultSet =  pst.executeQuery();
            while (resultSet.next()){
                int id = resultSet.getInt(1);
                String sectionName = resultSet.getString(4);
                int totalCapacity = resultSet.getInt(5);
                int leftCapacity = resultSet.getInt(6);
                CourseSection cur = new CourseSection();
                cur.id = id;
                cur.name = sectionName;
                cur.totalCapacity = totalCapacity;
                cur.leftCapacity = leftCapacity;
                courseSectionList.add(cur);
            }
            connection.commit();
            connection.close();

        }catch (Exception e){
            e.printStackTrace();
        }
        return courseSectionList;
    }

    @Override
    public Course getCourseBySection(int sectionId) {
        Course cur = new Course();
        try {
            Connection connection = SQLDataSource.getInstance().getSQLConnection();
            connection.setAutoCommit(false);
            connection.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
            String sql = "select count(*) from courseSection where id = ? ";
            PreparedStatement pst = connection.prepareStatement(sql);
            pst.setInt(1,sectionId);
            ResultSet resultSet =  pst.executeQuery();
            int exist = 0;
            if (resultSet.next()){
                exist = resultSet.getInt(1);
            }
            if (exist==0){
                connection.commit();
                connection.close();
                throw new EntityNotFoundException();
            }

            String sql1 = "select * from course where course.id in (select courseId from courseSection where courseSection.id = ? ) ";
            pst = connection.prepareStatement(sql1);
            pst.setInt(1,sectionId);
            resultSet =  pst.executeQuery();
           if (resultSet.next()){
               String id = resultSet.getString(1);
               String courseName = resultSet.getString(2);
               int credit = resultSet.getInt(3);
               int classHour = resultSet.getInt(4);
               String grade = resultSet.getString(5);
               Course.CourseGrading grading = Course.CourseGrading.HUNDRED_MARK_SCORE;
               if (grade.equals("HundredMarkScore")){
                   grading = Course.CourseGrading.HUNDRED_MARK_SCORE;
               }else if(grade.equals("PassOrFail")){
                   grading = Course.CourseGrading.PASS_OR_FAIL;
               }
               cur.id = id;
               cur.name = courseName;
               cur.credit = credit;
               cur.classHour = classHour;
               cur.grading = grading;
           }else{
               connection.commit();
               connection.close();
               throw new EntityNotFoundException();
           }
            connection.commit();
            connection.close();

        }catch (Exception e){
            e.printStackTrace();
        }
        return cur;
    }
    public static String contact(String firstname, String lastname){
        String fullname;
        if(firstname.matches("^[A-Za-z]+$") && lastname.matches("^[A-Za-z]+$"))
            fullname = firstname + " " + lastname;
        else
            fullname = firstname + lastname;
        return fullname;
    }
    @Override
    public List<CourseSectionClass> getCourseSectionClasses(int sectionId) {
        List<CourseSectionClass> courseSectionClassList = new ArrayList<CourseSectionClass>();
        try {
            Connection connection = SQLDataSource.getInstance().getSQLConnection();
            connection.setAutoCommit(false);
            connection.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
            String sql = "select count(*) from courseSection where id = ? ";
            PreparedStatement pst = connection.prepareStatement(sql);
            pst.setInt(1,sectionId);
            ResultSet resultSet =  pst.executeQuery();
            int exist = 0;
            if (resultSet.next()){
                exist = resultSet.getInt(1);
            }
            if (exist==0){
                connection.commit();
                connection.close();
                throw new EntityNotFoundException();
            }

            String sql1 = "select * from courseSectionClass where courseSectionId = ?";
            pst = connection.prepareStatement(sql1);
            pst.setInt(1,sectionId);
            resultSet =  pst.executeQuery();
            while (resultSet.next()){
                int id = resultSet.getInt(1);
                int instructorId = resultSet.getInt(3);
                int day = resultSet.getInt(4);
                Array array = resultSet.getArray(5);
                Object o = array.getArray();
                Short[] weeks = (Short[]) o;
                List<Short> resultList = List.of(weeks);
                int classStart = resultSet.getInt(6);
                int classEnd = resultSet.getInt(7);
                String location = resultSet.getString(8);
                CourseSectionClass cur = new CourseSectionClass();
                DayOfWeek dayOfWeek = null;
                if (day==1){
                    dayOfWeek = DayOfWeek.MONDAY;
                }else if (day==2){
                    dayOfWeek = DayOfWeek.TUESDAY;
                }else if (day==3){
                    dayOfWeek = DayOfWeek.WEDNESDAY;
                }else if (day==4){
                    dayOfWeek = DayOfWeek.THURSDAY;
                }else if (day==5){
                    dayOfWeek = DayOfWeek.FRIDAY;
                }else if (day==6){
                    dayOfWeek = DayOfWeek.SATURDAY;
                }else if (day==7){
                    dayOfWeek = DayOfWeek.SUNDAY;
                }

                cur.id = id;
                cur.instructor = new Instructor();
                cur.instructor.id = instructorId;
                String sql2 = "select * from instructor where id = ?";
                PreparedStatement pst0 = connection.prepareStatement(sql2);
                pst0.setInt(1,instructorId);
                ResultSet resultSet0 =  pst0.executeQuery();
                String fullName = null;
                if (resultSet0.next()){
                    String firstName = resultSet0.getString(1);
                    String lastName = resultSet0.getString(2);
                    fullName  = contact(firstName,lastName);
                }
                cur.instructor.fullName = fullName;
                cur.dayOfWeek = dayOfWeek;
                cur.weekList = new HashSet<>(resultList);
                cur.classBegin = (short) classStart;
                cur.classEnd = (short) classEnd;
                cur.location = location;
                courseSectionClassList.add(cur);
            }
            connection.commit();
            connection.close();

        }catch (Exception e){
            e.printStackTrace();
        }
        return courseSectionClassList;
    }

    @Override
    public CourseSection getCourseSectionByClass(int classId) {
        CourseSection cur = new CourseSection();
        try {
            Connection connection = SQLDataSource.getInstance().getSQLConnection();
            connection.setAutoCommit(false);
            connection.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
            String sql = "select count(*) from courseSectionClass where id = ? ";
            PreparedStatement pst = connection.prepareStatement(sql);
            pst.setInt(1,classId);
            ResultSet resultSet =  pst.executeQuery();
            int exist = 0;
            if (resultSet.next()){
                exist = resultSet.getInt(1);
            }
            if (exist==0){
                connection.commit();
                connection.close();
                throw new EntityNotFoundException();
            }

            String sql1 = "select * from courseSection where courseSection.id in (select courseSectionId from courseSectionClass where courseSectionClass.id = ? ) ";
            pst = connection.prepareStatement(sql1);
            pst.setInt(1,classId);
            resultSet =  pst.executeQuery();
            if (resultSet.next()){
                int id = resultSet.getInt(1);
                String courseSectionName = resultSet.getString(4);
                int totalCapacity = resultSet.getInt(5);
                int leftCapacity = resultSet.getInt(6);
                cur.id = id;
                cur.name = courseSectionName;
                cur.totalCapacity = totalCapacity;
                cur.leftCapacity = leftCapacity;
            }else{
                connection.commit();
                connection.close();
                throw new EntityNotFoundException();
            }
            connection.commit();
            connection.close();

        }catch (Exception e){
            e.printStackTrace();
        }
        return cur;
    }

    @Override
    public List<Student> getEnrolledStudentsInSemester(String courseId, int semesterId) {
        List<Student> studentList = new ArrayList<Student>();
        try {
            Connection connection = SQLDataSource.getInstance().getSQLConnection();
            connection.setAutoCommit(false);
            connection.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
            String sql = "select count(*) from course where id = ? ";
            PreparedStatement pst = connection.prepareStatement(sql);
            pst.setString(1,courseId);
            ResultSet resultSet =  pst.executeQuery();
            int exist = 0;
            if (resultSet.next()){
                exist = resultSet.getInt(1);
            }
            if (exist==0){
                connection.commit();
                connection.close();
                throw new EntityNotFoundException();
            }

            String sql1 = "select count(*) from semester where id = ? ";
            pst = connection.prepareStatement(sql1);
            pst.setInt(1,semesterId);
            resultSet =  pst.executeQuery();
            exist = 0;
            if (resultSet.next()){
                exist = resultSet.getInt(1);
            }
            if (exist==0){
                connection.commit();
                connection.close();
                throw new EntityNotFoundException();
            }

            String sql2 = "select student.id,student.firstname,student.lastname,student.enrolleddate,student.majorid,major.name,major.departmentid,department.name from student join Major on student.majorId = Major.id and student.id in (select studentId from studentGrades where studentGrades.sectionId in (select id from courseSection where courseSection.courseId = ? and courseSection.semesterId = ?)) join department on department.id = major.departmentId";
            pst = connection.prepareStatement(sql2);
            pst.setString(1,courseId);
            pst.setInt(2,semesterId);
            resultSet =  pst.executeQuery();
            while (resultSet.next()){
                int studentId = resultSet.getInt(1);
                String studentFirstName = resultSet.getString(2);
                String studentLastName = resultSet.getString(3);
                Date date = resultSet.getDate(4);
                int majorId = resultSet.getInt(5);
                String MajorName = resultSet.getString(6);
                int departmentId = resultSet.getInt(7);
                String departmentName = resultSet.getString(8);
                Department department = new Department();
                department.id = departmentId;
                department.name = departmentName;
                Major major = new Major();
                major.id = majorId;
                major.name = MajorName;
                major.department = department;
                Student student = new Student();
                student.enrolledDate = date;
                student.id = studentId;
                student.major = major;
                student.fullName = contact(studentFirstName,studentLastName);
                studentList.add(student);
            }
            connection.commit();
            connection.close();
        }catch (Exception e){
            e.printStackTrace();
        }
        return studentList;
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

}
