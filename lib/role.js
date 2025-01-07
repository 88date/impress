'use strict';

const checkRoles = (context, allowedRoles) => {
  const role = context?.session?.state?.role;
  return role && allowedRoles.includes(role);
};

module.exports = Object.freeze({ checkRoles });
