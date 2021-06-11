package Implementation;

import cn.edu.sustech.cs307.database.SQLDataSource;
import cn.edu.sustech.cs307.dto.Department;
import cn.edu.sustech.cs307.dto.Semester;
import cn.edu.sustech.cs307.exception.EntityNotFoundException;
import cn.edu.sustech.cs307.exception.IntegrityViolationException;
import cn.edu.sustech.cs307.service.SemesterService;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class ReferenceSemesterService implements SemesterService {
    @Override
    public int addSemester(String name, Date begin, Date end) {
        Integer enterInfoId = 1;
        try {
            if (begin.compareTo(end)>=0) {
                throw new IntegrityViolationException();
            }
            int exist = 0;
            Connection connection = SQLDataSource.getInstance().getSQLConnection();
            String sql="SELECT count(*) from semester where name = ? and begin=? and end1=?;";
            PreparedStatement pst = connection.prepareStatement(sql);
            pst.setString(1,name);
            pst.setDate(2,begin);
            pst.setDate(3,end);
            ResultSet resultSet = pst.executeQuery();
            if (resultSet.next()) {
                exist = resultSet.getInt(1);
            }
            if (exist>0) {
                connection.close();
                throw new IntegrityViolationException();
            }
            PreparedStatement stmt = connection.prepareStatement("insert into semester(name,begin,end1) values (?,?,?);", Statement.RETURN_GENERATED_KEYS);
            stmt.setString(1, name);
            stmt.setDate(2, begin);
            stmt.setDate(3, end);
            //stmt.execute();
            stmt.executeUpdate();
            ResultSet rst = stmt.getGeneratedKeys();
            if(rst.next()) {
                enterInfoId = rst.getInt(1);
                //System.out.print("获取自动增加的id号=="+enterInfoId+"\n");
            }
            connection.close();
        } catch (SQLException e) {

            e.printStackTrace();
        }
        return enterInfoId;
    }

    @Override
    public void removeSemester(int semesterId) {
        try {int exist = 0;
            Connection connection = SQLDataSource.getInstance().getSQLConnection();
            connection.setAutoCommit(false);
            connection.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
            String sql="SELECT count(*) from semester where id = ?";
            PreparedStatement pst = connection.prepareStatement(sql);
            pst.setInt(1,semesterId);
            ResultSet resultSet = pst.executeQuery();
            if (resultSet.next()) {
                exist = resultSet.getInt(1);
            }
            if (exist>0) {
                PreparedStatement stmt = connection.prepareStatement("delete from semester where id=?;");
                stmt.setInt(1, semesterId);
                stmt.execute();

                stmt = connection.prepareStatement("delete from CourseSectionclass where id= (select CourseSectionClass.id from CourseSectionClass join coursesection on\n" +
                        "    coursesection.id=coursesectionid where semesterId=?);");
                stmt.setInt(1, semesterId);
                stmt.execute();

                stmt = connection.prepareStatement("delete from CourseSearchEtry where id= (select CourseSearchEtry.id from coursesearchetry join coursesection on\n" +
                        "    coursesection.id=CourseSearchEtry.sectionId where semesterId=?);");
                stmt.setInt(1, semesterId);
                stmt.execute();

                stmt = connection.prepareStatement("delete from studentGrades where id=(select studentGrades.id from studentGrades join CourseSection on\n" +
                        "    sectionId=CourseSection.id where semesterId=?);");
                stmt.setInt(1, semesterId);
                stmt.execute();
                connection.commit();
                connection.close();
            }
            else
            {
                connection.commit();
                connection.close();
                throw new EntityNotFoundException();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Override
    public List<Semester> getAllSemesters() {
        ResultSet rst=null;
        List<Semester> slist=new ArrayList<>();
        try {Connection connection = SQLDataSource.getInstance().getSQLConnection();
            connection.setAutoCommit(false);
            connection.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
             String sql = "select * from semester; ";
             PreparedStatement pst = connection.prepareStatement(sql);

             ResultSet resultSet =  pst.executeQuery();
             while(resultSet.next()) {
                 int id=resultSet.getInt(1);
                 String name=resultSet.getString(2);
                 Date begin=resultSet.getDate(3);
                 Date end=resultSet.getDate(4);
                 Semester s=new Semester();
                 s.id=id;
                 s.name=name;
                 s.begin=begin;
                 s.end=end;
                 slist.add(s);
             }
            connection.commit();
             connection.close();
        }
        //Statement.RETURN_GENERATED_KEYS:获取自动增加的id号
        catch (SQLException e){
            e.printStackTrace();
        }
        return slist;
    }

    @Override
    public Semester getSemester(int semesterId) {
        ResultSet rst=null;
        Semester result=null;
        try {Connection connection = SQLDataSource.getInstance().getSQLConnection();
            connection.setAutoCommit(false);
            connection.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
             String sql = "select count(*) from semester where id = ? ";
             PreparedStatement pst = connection.prepareStatement(sql);
             pst.setInt(1,semesterId);
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

             PreparedStatement stmt = connection.prepareStatement("select * from semester where id=?");
            stmt.setInt(1,semesterId);
            stmt.executeUpdate();
            rst = stmt.getGeneratedKeys();
            rst=stmt.executeQuery("select * from semester where id=?");
            stmt.setInt(1,semesterId);
            result.id=(int)rst.getObject(1);
            result.name=(String)rst.getObject(2);
            result.begin=(Date)rst.getObject(3);
            result.end=(Date)rst.getObject(4);
            connection.commit();
            connection.close();
        }
        //Statement.RETURN_GENERATED_KEYS:获取自动增加的id号
        catch (SQLException e){
            e.printStackTrace();
        }
//        List a=(List)rst;
//        Department result=(Department) ((List) rst).get(0);
//        return result;
        return result;
    }
}
