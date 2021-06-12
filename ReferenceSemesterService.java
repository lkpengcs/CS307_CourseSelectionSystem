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
            Connection connection = SQLDataSource.getInstance().getSQLConnection();
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
                PreparedStatement stmt = connection.prepareStatement("delete from studentGrades where studentGrades.semesterId = ? ");
                stmt.setInt(1, semesterId);
                stmt.execute();
/*
                stmt = connection.prepareStatement("delete from CourseSearchEntry where id= (select CourseSearchEntry.id from coursesearchentry join coursesection on coursesection.id=CourseSearchEntry.sectionId where coursesection.semesterId=?)");
                stmt.setInt(1, semesterId);
                stmt.execute();*/

                stmt = connection.prepareStatement("delete from CourseSectionclass where id in (select CourseSectionClass.id from CourseSectionClass join coursesection on coursesection.id=courseSectionClass.coursesectionid where coursesection.semesterId=?)");
                stmt.setInt(1, semesterId);
                stmt.execute();

                stmt = connection.prepareStatement("delete from CourseSection where semesterId = ?");
                stmt.setInt(1, semesterId);
                stmt.execute();

                stmt = connection.prepareStatement("delete from semester where id = ?");
                stmt.setInt(1, semesterId);
                stmt.execute();

                connection.commit();
                connection.close();
            }
            else {
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
        List<Semester> slist=new ArrayList<>();
        try {
            Connection connection = SQLDataSource.getInstance().getSQLConnection();
            connection.setAutoCommit(false);
            connection.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
             String sql = "select * from semester ";
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
        Semester result= new Semester();
        try {
            Connection connection = SQLDataSource.getInstance().getSQLConnection();
            connection.setAutoCommit(false);
            connection.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
            String sql = "select * from semester where id = ? ";
            PreparedStatement pst = connection.prepareStatement(sql);
            pst.setInt(1, semesterId);
            ResultSet resultSet = pst.executeQuery();
            if (resultSet.next()) {
                result.id = resultSet.getInt(1);
                result.name = (String) resultSet.getObject(2);
                result.begin = (Date) resultSet.getObject(3);
                result.end = (Date) resultSet.getObject(4);
            } else {
                connection.commit();
                connection.close();
                throw new EntityNotFoundException();
            }
        }
        catch (SQLException e){
            e.printStackTrace();
        }
        return result;
    }
}
