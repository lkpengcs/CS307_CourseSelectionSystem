package Implementation;

import cn.edu.sustech.cs307.database.SQLDataSource;
import cn.edu.sustech.cs307.dto.Department;
import cn.edu.sustech.cs307.exception.EntityNotFoundException;
import cn.edu.sustech.cs307.exception.IntegrityViolationException;
import cn.edu.sustech.cs307.service.DepartmentService;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class ReferenceDepartmentService implements DepartmentService {
    @Override
    public int addDepartment(String name) {
        Integer enterInfoId = 1;
        try {
            Connection connection = SQLDataSource.getInstance().getSQLConnection();
            PreparedStatement stmt = connection.prepareStatement("insert into Department(name) values(?)", Statement.RETURN_GENERATED_KEYS);
            stmt.setString(1, name);
            //stmt.execute();
            stmt.executeUpdate();
            ResultSet rst = stmt.getGeneratedKeys();
            if (rst.next()) {
                enterInfoId = rst.getInt(1);
            }
            connection.close();
        } catch (SQLException e) {
            e.printStackTrace();
            throw new IntegrityViolationException();
        }
        return enterInfoId;
    }

    @Override
    public void removeDepartment(int departmentId) {
        try {
            int exist = 0;
            Connection connection = SQLDataSource.getInstance().getSQLConnection();
            connection.setAutoCommit(false);
            connection.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
            String sql = "SELECT count(*) from department where id = ?";
            PreparedStatement pst = connection.prepareStatement(sql);
            pst.setInt(1, departmentId);
            ResultSet resultSet = pst.executeQuery();
            if (resultSet.next()) {
                exist = resultSet.getInt(1);
            }
            if (exist > 0) {
                PreparedStatement stmt = connection.prepareStatement("delete from studentGrades where studentGrades.id in (select studentGrades.id from department join major on major.departmentId=Department.id join user1 on user1.majorid=major.id join studentGrades on studentGrades.studentid = user1.id where department.Id=?)");
                stmt.setInt(1,departmentId);
                stmt.execute();
                stmt = connection.prepareStatement("delete from major_course where major_course.majorid in (select major_course.majorid from department join major on major.departmentId=Department.id join major_course on major_course.majorid=major.id where department.Id=?)");
                stmt.setInt(1,departmentId);
                stmt.execute();
                //删掉对应major的student
                stmt = connection.prepareStatement("delete from user1 where user1.id in (select user1.id from department join major on major.departmentId=Department.id join user1 on user1.majorid=major.id where department.Id=?)");
                stmt.setInt(1, departmentId);
                stmt.execute();
                //删掉对应department的major
                stmt = connection.prepareStatement("delete from major where major.id in (select major.id from major join department on major.departmentId=department.id where department.Id=?)");
                stmt.setInt(1, departmentId);
                stmt.execute();
                stmt=connection.prepareStatement("delete from department where id=?");
                stmt.setInt(1, departmentId);
                stmt.execute();
            } else {
                connection.commit();
                connection.close();
                throw new EntityNotFoundException();
            }
            connection.commit();
            connection.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Override
    public List<Department> getAllDepartments() {
        List<Department> dlist=new ArrayList<>();
        try {
            Connection connection = SQLDataSource.getInstance().getSQLConnection();
            connection.setAutoCommit(false);
            connection.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
            String sql = "select * from department ";
            PreparedStatement pst = connection.prepareStatement(sql);

            ResultSet resultSet = pst.executeQuery();

            while(resultSet.next()){
                int id=resultSet.getInt(1);
                String name=resultSet.getString(2);
                Department d=new Department();
                d.id=id;
                d.name=name;
                dlist.add(d);
            }
            connection.commit();
            connection.close();
        }
        //Statement.RETURN_GENERATED_KEYS:获取自动增加的id号
        catch (SQLException e) {
            e.printStackTrace();
        }
        return dlist;
    }

    @Override
    public Department getDepartment(int departmentId) {
        Department result = new Department();
        try {
            Connection connection = SQLDataSource.getInstance().getSQLConnection();
            connection.setAutoCommit(false);
            connection.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
            PreparedStatement stmt = connection.prepareStatement("select * from department where id=?");
            stmt.setInt(1, departmentId);
            ResultSet resultSet =  stmt.executeQuery();
            if (resultSet.next()){
                result.id =  resultSet.getInt(1);
                result.name =  resultSet.getString(2);
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
}
