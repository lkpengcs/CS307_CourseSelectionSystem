package Implementation;

import cn.edu.sustech.cs307.database.SQLDataSource;
import cn.edu.sustech.cs307.dto.Course;
import cn.edu.sustech.cs307.dto.Department;
import cn.edu.sustech.cs307.dto.Major;
import cn.edu.sustech.cs307.exception.EntityNotFoundException;
import cn.edu.sustech.cs307.exception.IntegrityViolationException;
import cn.edu.sustech.cs307.service.*;


import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class ReferenceMajorService implements MajorService {
    @Override
    public int addMajor(String name, int departmentId) {
        Integer enterInfoId = 1;
        try {
            Connection connection = SQLDataSource.getInstance().getSQLConnection();
            PreparedStatement stmt = connection.prepareStatement("insert into major(name,departmentId) values (?,?)", Statement.RETURN_GENERATED_KEYS);
            stmt.setString(1, name);
            stmt.setInt(2, departmentId);
            stmt.executeUpdate();
            ResultSet rst = stmt.getGeneratedKeys();
            if (rst.next()) {
                enterInfoId = rst.getInt(1);
            }
            connection.close();
        } catch (SQLException e) {
            e.printStackTrace();
            throw new IntegrityViolationException();
            //RESERVE
        }
        return enterInfoId;


    }

    @Override
    public void removeMajor(int majorId) {
        try {
            int exist = 0;
            Connection connection = SQLDataSource.getInstance().getSQLConnection();
            connection.setAutoCommit(false);
            connection.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
            String sql="SELECT count(*) from major where id = ?";
            PreparedStatement pst = connection.prepareStatement(sql);
            pst.setInt(1,majorId);
            ResultSet resultSet = pst.executeQuery();
            if (resultSet.next()) {
                exist = resultSet.getInt(1);
            }
            if (exist>0) {
                PreparedStatement stmt = connection.prepareStatement("delete from major_course where majorid=?");
                stmt.setInt(1,majorId);
                stmt.executeUpdate();
                stmt = connection.prepareStatement("delete from studentGrades where studentGrades.studentId in( select studentGrades.studentId from studentGrades join user1 on user1.id = studentGrades.studentId where user1.majorid = ? )");
                stmt.setInt(1,majorId);
                stmt.executeUpdate();
                stmt = connection.prepareStatement("delete from user1 where user1.majorid = ?");
                stmt.setInt(1, majorId);
                stmt.execute();
                stmt = connection.prepareStatement("delete from major where id=?");
                stmt.setInt(1, majorId);
                stmt.execute();

                connection.commit();
                connection.close();
            }else{
                connection.commit();
                connection.close();
                throw new EntityNotFoundException();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Override
    public List<Major> getAllMajors() {
        List<Major> mlist=new ArrayList<>();
        try {
            Connection connection = SQLDataSource.getInstance().getSQLConnection();
            connection.setAutoCommit(false);
            connection.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
            String sql = "select major.id,major.name,major.departmentid,department.name from major join department on department.id = major.departmentid ";
            PreparedStatement pst = connection.prepareStatement(sql);
            ResultSet resultSet =  pst.executeQuery();
            while(resultSet.next()){
                int id=resultSet.getInt(1);
                String name=resultSet.getString(2);
                int did=resultSet.getInt(3);
                String dname = resultSet.getString(4);
                Major m=new Major();
                m.id=id;
                m.name=name;
                Department result=new Department();
                result.id = did;
                result.name = dname;
                m.department=result;
                mlist.add(m);
            }
            connection.commit();
            connection.close();
        }
        catch (SQLException e) {
            e.printStackTrace();
        }
        return mlist;


    }

    @Override
    public Major getMajor(int majorId) {
        Major result = new Major();

        try {
            Connection connection = SQLDataSource.getInstance().getSQLConnection();
            connection.setAutoCommit(false);
            connection.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
            PreparedStatement stmt = connection.prepareStatement("select major.id,major.name,major.departmentid,department.name from major join department on department.id = major.departmentid where major.id = ?");
            stmt.setInt(1, majorId);
            ResultSet resultSet = stmt.executeQuery();
            if (resultSet.next()) {
                result.id = resultSet.getInt(1);
                result.name = resultSet.getString(2);
                result.department = new Department();
                result.department.id = resultSet.getInt(3);
                result.department.name = resultSet.getString(4);
            }else {
                connection.commit();
                connection.close();
                throw new EntityNotFoundException();
            }
            connection.commit();
            connection.close();
        }
            catch (SQLException e) {
                e.printStackTrace();
            }
        return result;

    }
/*
    public boolean course_not_exist(String courseId) {
        ResultSet rst = null;
        Major result = null;
        int exist = 0;
        try {
//                Connection connection = SQLDataSource.getInstance().getSQLConnection();
//             PreparedStatement stmt = connection.prepareStatement("select * from course where id=?")) {
//            stmt.setString(1, courseId);
//            stmt.executeUpdate();
//            rst = stmt.getGeneratedKeys();
//            rst = stmt.executeQuery("select * from course where id=?");
//            stmt.setString(1, courseId);
//            connection.close();

            Connection connection = SQLDataSource.getInstance().getSQLConnection();
            connection.setAutoCommit(false);
            connection.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
            String sql = "SELECT count(*) from course where id = ?";
            PreparedStatement pst = connection.prepareStatement(sql);
            pst.setString(1, courseId);
            ResultSet resultSet = pst.executeQuery();
            if (resultSet.next()) {
                exist = resultSet.getInt(1);
            }
            connection.commit();
            connection.close();
            if (exist == 0) {
                throw new IntegrityViolationException();
            }

        }
        //Statement.RETURN_GENERATED_KEYS:获取自动增加的id号
        catch (SQLException e) {
            e.printStackTrace();
        }
//        List a=(List)rst;
//        Department result=(Department) ((List) rst).get(0);
//        return result;

        if (exist==0)
            return true;
        else
            return false;

    }

    public boolean major_not_exist(int Id) {
        ResultSet rst = null;
        Major result = null;
        int exist = 0;
        try {
//                Connection connection = SQLDataSource.getInstance().getSQLConnection();
//             PreparedStatement stmt = connection.prepareStatement("select * from course where id=?")) {
//            stmt.setString(1, courseId);
//            stmt.executeUpdate();
//            rst = stmt.getGeneratedKeys();
//            rst = stmt.executeQuery("select * from course where id=?");
//            stmt.setString(1, courseId);
//            connection.close();

            Connection connection = SQLDataSource.getInstance().getSQLConnection();
            connection.setAutoCommit(false);
            connection.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
            String sql = "SELECT count(*) from major where id = ?";
            PreparedStatement pst = connection.prepareStatement(sql);
            pst.setInt(1, Id);
            ResultSet resultSet = pst.executeQuery();
            if (resultSet.next()) {
                exist = resultSet.getInt(1);
            }
            connection.commit();
            connection.close();
            if (exist == 0) {
                throw new IntegrityViolationException();
            }

        }
        //Statement.RETURN_GENERATED_KEYS:获取自动增加的id号
        catch (SQLException e) {
            e.printStackTrace();
        }
//        List a=(List)rst;
//        Department result=(Department) ((List) rst).get(0);
//        return result;

        if (exist==0)
            return true;
        else
            return false;

    }
*/
    @Override
    public void addMajorCompulsoryCourse(int majorId, String courseId) {
        try {
            Connection connection = SQLDataSource.getInstance().getSQLConnection();
            PreparedStatement stmt = connection.prepareStatement("insert into major_course(majorId, courseId, type) VALUES (?,?,?)");
            stmt.setInt(1, majorId);
            stmt.setString(2, courseId);
            stmt.setInt(3, 1);
            stmt.executeUpdate();
            connection.close();
        } catch (SQLException e) {
            e.printStackTrace();
            throw new IntegrityViolationException();
        }
    }

    @Override
    public void addMajorElectiveCourse(int majorId, String courseId) {
        try {
            Connection connection = SQLDataSource.getInstance().getSQLConnection();
            PreparedStatement stmt = connection.prepareStatement("insert into major_course(majorId, courseId, type) VALUES (?,?,?)");
            stmt.setInt(1, majorId);
            stmt.setString(2, courseId);
            stmt.setInt(3, 2);
            stmt.executeUpdate();
            connection.close();
        } catch (SQLException e) {
            e.printStackTrace();
            throw new IntegrityViolationException();
        }
    }
}
